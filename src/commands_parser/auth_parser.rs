use super::RedisCommand;

pub fn parse(command: &str, args: &[&str]) -> Result<RedisCommand, String> {
    match command {
        "acl" => parse_acl(args),
        _ => Err(format!("Unknown command: {}", command)),
    }
}

fn parse_acl(args: &[&str]) -> Result<RedisCommand, String> {
    if args.is_empty() {
        return Err("ACL requires a subcommand".into());
    }

    Ok(RedisCommand::Acl {
        subcommand: args[0].to_string(),
        arguments: args[1..].iter().map(|arg| arg.to_string()).collect(),
    })
}