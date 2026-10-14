use crate::engine::{BranchId, EpistemicState, TxTimestamp};

#[derive(Debug)]
pub enum DslCommand {
    Fork { new_branch: BranchId, parent: BranchId },
    Commit { branch: BranchId, state: EpistemicState, deps: Vec<TxTimestamp>, payload: String },
    Refute { branch: BranchId, target_deps: Vec<TxTimestamp>, reason: String },
    Recall { branch: BranchId, top_k: usize, query: String },
    Merge { src_branch: BranchId, dst_branch: BranchId },
    Benchmark { count: usize },
    Compact { threshold: f32 },
    Status,
    Help,
}

pub fn parse_command(input: &str) -> Result<DslCommand, String> {
    let input = input.trim();
    if input.is_empty() { return Err("Empty command".into()); }
    let upper = input.to_uppercase();
    if upper == "STATUS" { return Ok(DslCommand::Status); }
    if upper == "HELP" { return Ok(DslCommand::Help); }

    if upper.starts_with("FORK ") {
        let parts: Vec<&str> = input.split_whitespace().collect();
        if parts.len() != 4 || parts[2].to_uppercase() != "FROM" {
            return Err("Syntax: FORK <new_branch_id> FROM <parent_branch_id>".into());
        }
        let new_b = parts[1].parse::<u64>().map_err(|_| "Invalid new_branch_id")?;
        let parent_b = parts[3].parse::<u64>().map_err(|_| "Invalid parent_branch_id")?;
        return Ok(DslCommand::Fork { new_branch: BranchId(new_b), parent: BranchId(parent_b) });
    }

    if upper.starts_with("MERGE ") {
        let parts: Vec<&str> = input.split_whitespace().collect();
        if parts.len() != 4 || parts[2].to_uppercase() != "INTO" {
            return Err("Syntax: MERGE <src_branch_id> INTO <dst_branch_id>".into());
        }
        let src_b = parts[1].parse::<u64>().map_err(|_| "Invalid src_branch_id")?;
        let dst_b = parts[3].parse::<u64>().map_err(|_| "Invalid dst_branch_id")?;
        return Ok(DslCommand::Merge { src_branch: BranchId(src_b), dst_branch: BranchId(dst_b) });
    }

    if upper.starts_with("BENCHMARK") {
        let parts: Vec<&str> = input.split_whitespace().collect();
        let count = if parts.len() > 1 { parts[1].parse::<usize>().unwrap_or(25000) } else { 25000 };
        return Ok(DslCommand::Benchmark { count });
    }

    if upper.starts_with("COMPACT") {
        let parts: Vec<&str> = input.split_whitespace().collect();
        let threshold = if parts.len() > 1 { parts[1].parse::<f32>().unwrap_or(0.55) } else { 0.55 };
        return Ok(DslCommand::Compact { threshold });
    }

    if upper.starts_with("RECALL ") {
        let payload = extract_quoted(input)?;
        let prefix = &input[..input.find('"').unwrap()];
        let parts: Vec<&str> = prefix.split_whitespace().collect();
        if parts.len() < 5 || parts[1].to_uppercase() != "ON" || parts[3].to_uppercase() != "TOP" {
            return Err("Syntax: RECALL ON <branch_id> TOP <k> \"<search query>\"".into());
        }
        let branch = parts[2].parse::<u64>().map_err(|_| "Invalid branch_id")?;
        let top_k = parts[4].parse::<usize>().map_err(|_| "Invalid top_k")?;
        return Ok(DslCommand::Recall { branch: BranchId(branch), top_k, query: payload });
    }

    if upper.starts_with("REFUTE ") {
        let reason = extract_quoted(input)?;
        let deps = extract_brackets(input)?;
        let prefix = &input[..input.find('"').unwrap()];
        let on_idx = prefix.to_uppercase().find("ON ").ok_or("Missing ON <branch>")?;
        let branch_str = prefix[on_idx + 3..].trim();
        let branch = branch_str.parse::<u64>().map_err(|_| "Invalid branch_id")?;
        return Ok(DslCommand::Refute { branch: BranchId(branch), target_deps: deps, reason });
    }

    if upper.starts_with("COMMIT ") {
        let payload = extract_quoted(input)?;
        let deps = extract_brackets(input).unwrap_or_default();
        let prefix = &input[..input.find('"').unwrap()];
        let parts: Vec<&str> = prefix.split_whitespace().collect();
        if parts.len() < 4 {
            return Err("Syntax: COMMIT FACT|HYP(0.8) ON <branch> DEPS [1,2] \"<payload>\"".into());
        }
        let state_token = parts[1].to_uppercase();
        let state = if state_token == "FACT" {
            EpistemicState::VerifiedFact
        } else if state_token.starts_with("HYP(") && state_token.ends_with(')') {
            let conf_str = &state_token[4..state_token.len() - 1];
            let conf = conf_str.parse::<f32>().map_err(|_| "Invalid confidence float")?;
            EpistemicState::Hypothesis { confidence: conf }
        } else {
            return Err("State must be FACT or HYP(0.75)".into());
        };
        let on_pos = parts.iter().position(|w| w.to_uppercase() == "ON").ok_or("Missing ON")?;
        let branch = parts.get(on_pos + 1).ok_or("Missing branch_id")?.parse::<u64>().map_err(|_| "Invalid branch_id")?;
        return Ok(DslCommand::Commit { branch: BranchId(branch), state, deps, payload });
    }

    Err("Unrecognized DSL command. Type HELP for syntax.".into())
}

fn extract_quoted(s: &str) -> Result<String, String> {
    let first = s.find('"').ok_or("Missing opening quote")?;
    let last = s.rfind('"').ok_or("Missing closing quote")?;
    if first == last { return Err("Unclosed quote".into()); }
    Ok(s[first + 1..last].to_string())
}

fn extract_brackets(s: &str) -> Result<Vec<TxTimestamp>, String> {
    let open = match s.find('[') { Some(idx) => idx, None => return Ok(Vec::new()) };
    let close = s.find(']').ok_or("Missing closing bracket")?;
    let inner = s[open + 1..close].trim();
    if inner.is_empty() { return Ok(Vec::new()); }
    let mut out = Vec::new();
    for part in inner.split(',') {
        let id = part.trim().parse::<u64>().map_err(|_| "Invalid Tx ID")?;
        out.push(TxTimestamp(id));
    }
    Ok(out)
}
