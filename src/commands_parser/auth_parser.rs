use super::RedisCommand;

pub fn parse(command: &str, args: &[&str]) -> Result<RedisCommand, String> {
    match command {
        "acl" => parse_acl(args),
        "auth" => parse_auth(args),
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
    })}
    fn parse_auth(args: &[&str]) -> Result<RedisCommand, String> {
        if args.len() < 2 {
            return Err("AUTH requires username and password".into());
        }

        Ok(RedisCommand::Auth {
            username: args[0].to_string(),
            password: args[1].to_string(),
        })
    }
