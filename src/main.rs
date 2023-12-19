use std::{fs::File, io::Write as _, time::Duration};

use anyhow::anyhow;
use clap::Parser;
use serde_derive::Deserialize;
use serde_json::{self as json, json};
use url::Url;

#[derive(Debug, Parser)]
#[clap(
    name = "qtrace",
    version = env!("CARGO_PKG_VERSION"),
    author = env!("CARGO_PKG_AUTHORS"),
    about = "Obtain slow query traces from the hosted service"
)]
struct Opts {
    /// The config file to use
    #[clap(short, long, default_value = "config.toml", env = "QTRACE_CONFIG")]
    config: String,
    /// The `query_id` to trace
    #[clap(short, long)]
    qid: Option<String>,
    /// Only consider queries that took longer than this many milliseconds
    #[clap(short, long)]
    min_time: Option<usize>,
    /// Print some more information
    #[clap(short, long)]
    verbose: bool,
    /// Save the output in this file
    #[clap(short, long)]
    data: Option<String>,
    /// Save the query trace in this file
    #[clap(short, long)]
    trace: Option<String>,
    /// The IPFS hash of the deployment
    #[clap(required = true)]
    deployment: String,
}

#[derive(Debug)]
struct LogEntry {
    query: String,
    variables: json::Value,
}

#[derive(Deserialize, Debug)]
struct Loki {
    cluster: String,
    url: String,
    username: String,
    password: String,
}

impl Loki {
    fn query_url(&self) -> anyhow::Result<Url> {
        let mut url = Url::parse(&self.url)?;
        url.set_username(&self.username)
            .map_err(|_| anyhow!("Failed to set Loki username"))?;
        url.set_password(Some(&self.password))
            .map_err(|_| anyhow!("Failed to set Loki password"))?;
        url.set_path("/loki/api/v1/query");
        Ok(url)
    }

    fn query(
        &self,
        deployment: &str,
        qid: Option<&str>,
        min_time: Option<usize>,
    ) -> anyhow::Result<LogEntry> {
        let query = {
            // This will need to be adjusted if the query log format changes
            const PATTERN: &str = r#"pattern "<_>INFO Query timing (GraphQL), block: <block>, query_time_ms: <query_time>, variables: <variables>, query: <query> , query_id: <query_id>,""#;
            let mut query = format!(
                r#"{{cluster="{cluster}",app=~"query-node.*",deployment="{deployment}",container="query-node"}} | {PATTERN}"#,
                cluster = self.cluster
            );
            if let Some(qid) = qid {
                query.push_str(&format!(r#" | query_id="{qid}""#));
            }
            if let Some(min_time) = min_time {
                query.push_str(&format!(r#" | query_time > {min_time}"#));
            }
            query
        };

        let url = self.query_url()?;
        let client = reqwest::blocking::Client::new();
        let resp = client
            .get(url)
            .query(&[("query", query.as_str()), ("limit", "1")])
            .send()
            .map_err(|e| anyhow!("Failed to send Loki query: {}", e))?
            .text()
            .map_err(|e| anyhow!("Failed to get Loki response: {}", e))?;
        let resp: json::Value =
            json::from_str(&resp).map_err(|e| anyhow!("Failed to parse Loki response: {}", e))?;
        let stream = match &resp["data"]["result"][0]["stream"] {
            json::Value::Object(o) => o,
            _ => return Err(anyhow!("Invalid Loki response: could not find stream")),
        };
        let query = match &stream["query"] {
            json::Value::String(s) => s.to_string(),
            _ => return Err(anyhow!("Invalid Loki response: could not find query")),
        };
        let variables = match &stream["variables"] {
            json::Value::String(s) => json::from_str(s)?,
            _ => return Err(anyhow!("Invalid Loki response: could not find variables")),
        };
        let entry = LogEntry { query, variables };
        Ok(entry)
    }
}

#[derive(Debug)]
pub enum Trace {
    Root {
        query: String,
        variables: String,
        query_id: String,
        block: usize,
        elapsed: Duration,
        conn_wait: Duration,
        permit_wait: Duration,
        children: Vec<(String, Trace)>,
    },
    Query {
        query: String,
        elapsed: Duration,
        conn_wait: Duration,
        permit_wait: Duration,
        entity_count: usize,
        children: Vec<(String, Trace)>,
    },
}

impl Trace {
    fn number_as_millis(entry: &json::Value, key: &str) -> anyhow::Result<Duration> {
        entry[key]
            .as_u64()
            .ok_or_else(|| anyhow!("Invalid trace: {key} is not a duration"))
            .map(Duration::from_millis)
    }

    fn parse(root: &json::Value) -> anyhow::Result<Self> {
        let mut children = Vec::new();
        for (key, value) in root
            .as_object()
            .ok_or_else(|| anyhow!("Invalid trace: root is not an object"))?
        {
            if value.is_object() {
                children.push(Self::parse_query(key, value)?);
            }
        }
        Ok(Self::Root {
            query: root["query"].to_string(),
            variables: root["variables"].to_string(),
            query_id: root["query_id"].to_string(),
            block: root["block"]
                .as_u64()
                .ok_or_else(|| anyhow!("Invalid trace: block is not a number"))?
                as usize,
            elapsed: Self::number_as_millis(root, "elapsed_ms")?,
            conn_wait: Self::number_as_millis(root, "conn_wait_ms")?,
            permit_wait: Self::number_as_millis(root, "permit_wait_ms")?,
            children,
        })
    }

    fn parse_query(name: &str, query: &json::Value) -> anyhow::Result<(String, Self)> {
        let mut children = Vec::new();
        for (key, value) in query
            .as_object()
            .ok_or_else(|| anyhow!("Invalid trace: {name} is not an object"))?
        {
            if value.is_object() {
                children.push(Self::parse_query(key, value)?);
            }
        }
        let elapsed = Self::number_as_millis(query, "elapsed_ms")?;
        let conn_wait = Self::number_as_millis(query, "conn_wait_ms")?;
        let permit_wait = Self::number_as_millis(query, "permit_wait_ms")?;
        let entity_count = query["entity_count"]
            .as_u64()
            .ok_or_else(|| anyhow!("Invalid trace: entity count is not a number"))?
            as usize;
        let query = Self::Query {
            query: query.to_string(),
            elapsed,
            conn_wait,
            permit_wait,
            entity_count,
            children,
        };
        Ok((name.to_string(), query))
    }

    fn query_id(&self) -> &str {
        match self {
            Self::Root { query_id, .. } => query_id,
            Self::Query { .. } => "none",
        }
    }
}

#[derive(Deserialize, Debug)]
struct GraphNode {
    url: String,
    #[serde(rename = "trace-token")]
    trace_token: String,
}

impl GraphNode {
    fn query_url(&self, deployment: &str) -> anyhow::Result<Url> {
        let mut url = Url::parse(&self.url)?;
        url.set_path(&format!("/subgraphs/id/{deployment}"));
        Ok(url)
    }

    fn query(&self, deployment: &str, log_entry: &LogEntry) -> anyhow::Result<json::Value> {
        let url = self.query_url(deployment)?;
        let client = reqwest::blocking::Client::new();
        let body = json! {
            {
                "query": log_entry.query,
                "variables": log_entry.variables,
            }
        }
        .to_string();

        let resp = client
            .post(url)
            .header("X-GraphTraceQuery", &self.trace_token)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .map_err(|e| anyhow!("Failed to send graph-node query: {}", e))?
            .text()
            .map_err(|e| anyhow!("Failed to get graph-node response: {}", e))?;
        json::from_str(&resp).map_err(|e| anyhow!("Failed to parse graph-node response: {}", e))
    }
}

#[derive(Deserialize, Debug)]
struct Output {
    trace: Option<String>,
    data: Option<String>,
    query: Option<String>,
    variables: Option<String>,
}

#[derive(Deserialize, Debug)]
struct Config {
    loki: Loki,
    #[serde(rename = "graph-node")]
    graph_node: GraphNode,
    output: Option<Output>,
}

impl Config {
    fn load(file: &str) -> anyhow::Result<Config> {
        let config = std::fs::read_to_string(file)?;
        let config: Config = toml::from_str(&config)?;
        Ok(config)
    }
}

fn save_query(config: &Config, log_entry: &LogEntry) -> anyhow::Result<()> {
    if let Some(output) = &config.output {
        if let Some(query) = &output.query {
            let mut f = File::create(query)?;
            writeln!(f, "{}", log_entry.query)?;
        }
        if let Some(vars) = &output.variables {
            let mut f = File::create(vars)?;
            writeln!(f, "{}", json::to_string_pretty(&log_entry.variables)?)?;
        }
    }
    Ok(())
}

fn save_output(opt: &Opts, config: &Config, json_output: &json::Value) -> anyhow::Result<()> {
    let output = opt.data.as_ref().or(config
        .output
        .as_ref()
        .and_then(|output| output.data.as_ref()));

    if let Some(output) = &output {
        let mut f = File::create(output)?;
        let json = json::to_string_pretty(&json_output["data"])?;
        writeln!(f, "{}", json)?;
    }
    Ok(())
}

fn save_trace(opt: &Opts, config: &Config, json_trace: &json::Value) -> anyhow::Result<()> {
    let trace = opt.trace.as_ref().or(config
        .output
        .as_ref()
        .and_then(|output| output.trace.as_ref()));

    if let Some(trace) = trace {
        let mut f = File::create(trace)?;
        let json = json::to_string_pretty(json_trace)?;
        writeln!(f, "{}", json)?;
    }
    Ok(())
}

fn print_brief_trace(name: &str, trace: &Trace, indent: usize) -> Result<(), anyhow::Error> {
    use Trace::*;

    fn query_time(trace: &Trace) -> Duration {
        match trace {
            Root { children, .. } => children.iter().map(|(_, trace)| query_time(trace)).sum(),
            Query {
                elapsed, children, ..
            } => *elapsed + children.iter().map(|(_, trace)| query_time(trace)).sum(),
        }
    }

    match trace {
        Root {
            elapsed, children, ..
        } => {
            let qt = query_time(trace);
            let pt = *elapsed - qt;

            println!(
                "{space:indent$}{name:rest$} {elapsed:7}ms",
                space = " ",
                indent = indent,
                rest = 48 - indent,
                name = name,
                elapsed = elapsed.as_millis(),
            );
            for (name, trace) in children {
                print_brief_trace(name, trace, indent + 2)?;
            }
            println!("\nquery:      {:7}ms", qt.as_millis());
            println!("other:      {:7}ms", pt.as_millis());
            println!("total:      {:7}ms", elapsed.as_millis())
        }
        Query {
            elapsed,
            entity_count,
            children,
            ..
        } => {
            println!(
                "{space:indent$}{name:rest$} {elapsed:7}ms [{count:7} entities]",
                space = " ",
                indent = indent,
                rest = 50 - indent,
                name = name,
                elapsed = elapsed.as_millis(),
                count = entity_count
            );
            for (name, trace) in children {
                print_brief_trace(name, trace, indent + 2)?;
            }
        }
    }

    Ok(())
}

fn main() -> anyhow::Result<()> {
    let opt = Opts::parse();
    let config = Config::load(&opt.config)?;
    let mut out: Box<dyn std::io::Write> = if opt.verbose {
        Box::new(std::io::stdout())
    } else {
        Box::new(std::io::sink())
    };

    writeln!(out, "Querying Loki for query log entry")?;
    let log_entry = config
        .loki
        .query(&opt.deployment, opt.qid.as_deref(), opt.min_time)?;
    save_query(&config, &log_entry)?;

    writeln!(out, "Querying graph-node for query trace")?;
    let output = &config.graph_node.query(&opt.deployment, &log_entry)?;
    save_output(&opt, &config, output)?;

    let trace = &output["trace"];
    save_trace(&opt, &config, trace)?;

    let trace = Trace::parse(trace)?;
    println!(
        "Trace for qid {}\n deployment {}\n",
        trace.query_id(),
        opt.deployment
    );
    print_brief_trace("root", &trace, 0)?;
    Ok(())
}
