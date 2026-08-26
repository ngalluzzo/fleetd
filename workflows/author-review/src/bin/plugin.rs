use std::io::{self, Write};
use std::process::ExitCode;

use fleetd_author_review::{
    plugin::{describe, evaluate},
    protocol::{EvaluateParams, MAX_FRAME_BYTES, RpcRequest, RpcResponse},
};
use serde_json::Value;

fn main() -> ExitCode {
    let stdin = io::stdin();
    let mut input = stdin.lock();
    let stdout = io::stdout();
    let mut output = stdout.lock();
    loop {
        let frame = match read_bounded_line(&mut input) {
            Ok(Some(frame)) => frame,
            Ok(None) => return ExitCode::SUCCESS,
            Err(error) => {
                let _ = write_response(
                    &mut output,
                    &RpcResponse::failure(0, -32_700, error.to_string()),
                );
                return ExitCode::FAILURE;
            }
        };
        let request: RpcRequest = match serde_json::from_slice(&frame) {
            Ok(request) => request,
            Err(error) => {
                if write_response(
                    &mut output,
                    &RpcResponse::failure(0, -32_700, format!("invalid JSON request: {error}")),
                )
                .is_err()
                {
                    return ExitCode::FAILURE;
                }
                continue;
            }
        };
        let response = dispatch(request);
        if write_response(&mut output, &response).is_err() {
            return ExitCode::FAILURE;
        }
    }
}

fn dispatch(request: RpcRequest) -> RpcResponse {
    if request.jsonrpc != "2.0" {
        return RpcResponse::failure(request.id, -32_600, "jsonrpc must equal 2.0");
    }
    match request.method.as_str() {
        "workflow.describe" => {
            if request.params != Value::Object(serde_json::Map::new()) {
                return RpcResponse::failure(
                    request.id,
                    -32_602,
                    "workflow.describe params must be an empty object",
                );
            }
            match serde_json::to_value(describe()) {
                Ok(result) => RpcResponse::success(request.id, result),
                Err(error) => RpcResponse::failure(request.id, -32_603, error.to_string()),
            }
        }
        "workflow.evaluate" => {
            let params: EvaluateParams = match serde_json::from_value(request.params) {
                Ok(params) => params,
                Err(error) => {
                    return RpcResponse::failure(
                        request.id,
                        -32_602,
                        format!("invalid workflow.evaluate params: {error}"),
                    );
                }
            };
            match evaluate(&params).and_then(|result| {
                serde_json::to_value(result).map_err(|error| {
                    fleetd_author_review::plugin::AuthorReviewError::Evaluation(error.to_string())
                })
            }) {
                Ok(result) => RpcResponse::success(request.id, result),
                Err(error) => RpcResponse::failure(request.id, -32_010, error.to_string()),
            }
        }
        _ => RpcResponse::failure(request.id, -32_601, "unknown workflow method"),
    }
}

fn read_bounded_line(reader: &mut impl io::BufRead) -> io::Result<Option<Vec<u8>>> {
    let mut frame = Vec::new();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return if frame.is_empty() {
                Ok(None)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "workflow frame ended without a newline",
                ))
            };
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let take = newline.map_or(available.len(), |position| position + 1);
        if frame.len().saturating_add(take) > MAX_FRAME_BYTES + 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "workflow frame exceeded its bound",
            ));
        }
        frame.extend_from_slice(&available[..take]);
        reader.consume(take);
        if newline.is_some() {
            frame.pop();
            return Ok(Some(frame));
        }
    }
}

fn write_response(writer: &mut impl Write, response: &RpcResponse) -> io::Result<()> {
    let bytes = serde_json::to_vec(response).map_err(io::Error::other)?;
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "workflow response exceeded its bound",
        ));
    }
    writer.write_all(&bytes)?;
    writer.write_all(b"\n")?;
    writer.flush()
}
