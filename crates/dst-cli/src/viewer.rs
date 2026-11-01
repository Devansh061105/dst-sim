//! Exports a run for the trace viewer: JSON data, or the viewer page with the data built in.

use std::fmt::Write;

use checker::Verdict;
use harness::{OpRecord, Outcome, SERVERS, Scenario};
use kv::{Op, OpResult};
use sim_core::{DropReason, EventKind, Fault, TraceEvent};

/// The viewer page (`viewer/index.html`); `dst run --html` writes it with the run built in.
const PAGE: &str = include_str!("../../../viewer/index.html");
/// Where the page expects the data.
const SLOT: &str = "/*TRACE_DATA*/null";

/// The viewer page with this run's data built in: a single file that opens in any browser.
pub fn page(scenario: &Scenario, outcome: &Outcome) -> String {
    assert!(PAGE.contains(SLOT), "viewer page lost its data slot");
    PAGE.replacen(SLOT, &json(scenario, outcome), 1)
}

/// The run as JSON. Times are virtual nanoseconds.
pub fn json(scenario: &Scenario, outcome: &Outcome) -> String {
    let mut out = String::from("{");
    let bug = scenario.bug.map(|b| format!(", bug {b}")).unwrap_or_default();
    let scripted = if scenario.script.is_some() { ", scripted faults" } else { "" };
    let title = format!("seed {} ({}{bug}{scripted})", scenario.seed, scenario.mode);
    let _ = write!(out, "\"title\":{},", string(&title));
    let failure = outcome.failure().map_or_else(|| "null".to_owned(), |f| string(&f));
    let _ = write!(out, "\"failure\":{failure},\"end\":{},", outcome.end_time.as_nanos());
    let _ = write!(
        out,
        "\"summary\":{},",
        string(&format!(
            "{} events, {} operations, {} entries committed, max term {}",
            outcome.events, outcome.operations, outcome.committed, outcome.max_term
        ))
    );

    out.push_str("\"nodes\":[");
    for id in 0..scenario.nodes() {
        let role = if id < SERVERS { "server" } else { "client" };
        let _ = write!(out, "{}{{\"id\":{id},\"role\":\"{role}\"}}", comma(id as usize));
    }
    out.push_str("],");

    let bad: &[usize] = match &outcome.verdict {
        Verdict::NotLinearizable { ops, .. } => ops,
        _ => &[],
    };
    out.push_str("\"ops\":[");
    for (i, op) in outcome.ops.iter().enumerate() {
        out.push_str(comma(i));
        op_json(&mut out, op, bad.contains(&i));
    }
    out.push_str("],");

    out.push_str("\"events\":[");
    for (i, event) in outcome.trace.iter().flatten().enumerate() {
        out.push_str(comma(i));
        event_json(&mut out, event);
    }
    out.push_str("]}");
    out
}

fn op_json(out: &mut String, op: &OpRecord, bad: bool) {
    let (end, result) = match &op.completed {
        Some((at, result)) => (at.as_nanos().to_string(), describe_result(result)),
        None => ("null".to_owned(), "no answer".to_owned()),
    };
    let text = format!("{} -> {result}", describe_op(&op.op));
    let _ = write!(
        out,
        "{{\"client\":{},\"start\":{},\"end\":{end},\"text\":{},\"bad\":{bad}}}",
        op.client.0,
        op.invoked.as_nanos(),
        string(&text)
    );
}

fn event_json(out: &mut String, event: &TraceEvent) {
    let _ = write!(out, "{{\"t\":{},", event.at.as_nanos());
    match &event.kind {
        EventKind::Start { node } => _ = write!(out, "\"k\":\"start\",\"node\":{}", node.0),
        EventKind::Deliver { from, to, sent } => {
            let (from, to, sent) = (from.0, to.0, sent.as_nanos());
            _ = write!(out, "\"k\":\"deliver\",\"from\":{from},\"to\":{to},\"sent\":{sent}");
        }
        EventKind::Timer { node } => _ = write!(out, "\"k\":\"timer\",\"node\":{}", node.0),
        EventKind::Drop { from, to, sent, reason } => {
            let reason = match reason {
                DropReason::Lost => "lost",
                DropReason::Partitioned => "partitioned",
                DropReason::NodeDown => "node down",
            };
            let (from, to, sent) = (from.0, to.0, sent.as_nanos());
            _ = write!(
                out,
                "\"k\":\"drop\",\"from\":{from},\"to\":{to},\"sent\":{sent},\"reason\":\"{reason}\""
            );
        }
        EventKind::Fault(fault) => {
            let (name, nodes) = match fault {
                Fault::Heal => ("heal", Vec::new()),
                Fault::Split(side) => ("split", side.clone()),
                Fault::OneWay { from, to } => ("one-way", vec![*from, *to]),
                Fault::Crash(node) => ("crash", vec![*node]),
                Fault::Restart(node) => ("restart", vec![*node]),
            };
            let nodes: Vec<String> = nodes.iter().map(|n| n.0.to_string()).collect();
            _ = write!(out, "\"k\":\"fault\",\"fault\":\"{name}\",\"nodes\":[{}]", nodes.join(","));
        }
        EventKind::DiskCrash { node } => _ = write!(out, "\"k\":\"disk\",\"node\":{}", node.0),
        EventKind::SyncFailed { node } => _ = write!(out, "\"k\":\"sync\",\"node\":{}", node.0),
        EventKind::Halt { node } => _ = write!(out, "\"k\":\"halt\",\"node\":{}", node.0),
        EventKind::Corrupt { node } => _ = write!(out, "\"k\":\"corrupt\",\"node\":{}", node.0),
    }
    let _ = write!(out, ",\"text\":{}}}", string(&event.text));
}

fn describe_op(op: &Op) -> String {
    match *op {
        Op::Get { key } => format!("get k{key}"),
        Op::Put { key, value } => format!("put k{key} = {value}"),
        Op::Cas { key, expected: Some(old), new } => format!("cas k{key} {old} -> {new}"),
        Op::Cas { key, expected: None, new } => format!("cas k{key} (absent) -> {new}"),
    }
}

fn describe_result(result: &OpResult) -> String {
    match result {
        OpResult::Value(Some(v)) => v.to_string(),
        OpResult::Value(None) => "absent".to_owned(),
        OpResult::Written => "ok".to_owned(),
        OpResult::Cas { swapped: true } => "swapped".to_owned(),
        OpResult::Cas { swapped: false } => "not swapped".to_owned(),
    }
}

fn comma(i: usize) -> &'static str {
    if i == 0 { "" } else { "," }
}

/// A JSON string literal. `<` is escaped too, so the JSON can sit inside a `<script>` element.
fn string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '<' => out.push_str("\\u003c"),
            c if (c as u32) < 0x20 => _ = write!(out, "\\u{:04x}", c as u32),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_has_the_run_built_in() {
        let scenario = Scenario::new(harness::Mode::Chaos, 1, None);
        let outcome = scenario.run(true);
        let page = page(&scenario, &outcome);
        assert!(!page.contains(SLOT));
        assert!(page.contains(r#""title":"seed 1 (chaos)""#));
        assert!(page.contains(r#""k":"deliver""#) && page.contains(r#""k":"fault""#));
        // The data can't end the page's script early.
        assert_eq!(page.matches("</script>").count(), PAGE.matches("</script>").count());
    }

    #[test]
    fn strings_are_escaped_for_json_inside_html() {
        assert_eq!(string("a\"b\\c\n</script>"), r#""a\"b\\c\n\u003c/script>""#);
        assert_eq!(string("\u{1}"), r#""\u0001""#);
    }
}
