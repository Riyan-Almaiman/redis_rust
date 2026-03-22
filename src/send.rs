use crate::resp::Resp;
use tokio::sync::mpsc::UnboundedSender;

pub fn send_cmd(response_tx: UnboundedSender<Vec<u8>>, resp: Resp) {
    let mut buffer = Vec::new();

    resp.write_format(&mut buffer);
    let _ = response_tx.send(buffer);
}
