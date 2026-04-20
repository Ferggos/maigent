#include "maigent/common/logging.h"

#include <filesystem>
#include <iostream>

#include "maigent/common/time_utils.h"

namespace maigent {

namespace {

std::string Escape(const std::string& value) {
  std::string out;
  out.reserve(value.size());
  for (char c : value) {
    if (c == '"') {
      out += "\\\"";
    } else if (c == '\\') {
      out += "\\\\";
    } else if (c == '\n') {
      out += "\\n";
    } else {
      out += c;
    }
  }
  return out;
}

}  // namespace

AgentLogger::AgentLogger(std::string agent_id, std::string file_path, bool also_stderr)
    : agent_id_(std::move(agent_id)),
      file_path_(std::move(file_path)),
      also_stderr_(also_stderr) {
  std::filesystem::path p(file_path_);
  if (!p.parent_path().empty()) {
    std::error_code ec;
    std::filesystem::create_directories(p.parent_path(), ec);
  }
  out_.open(file_path_, std::ios::out | std::ios::app);
}

void AgentLogger::Info(const std::string& message, const LogContext& ctx) {
  Log("INFO", message, ctx);
}

void AgentLogger::Warn(const std::string& message, const LogContext& ctx) {
  Log("WARN", message, ctx);
}

void AgentLogger::Error(const std::string& message, const LogContext& ctx) {
  Log("ERROR", message, ctx);
}

void AgentLogger::Debug(const std::string& message, const LogContext& ctx) {
  Log("DEBUG", message, ctx);
}

void AgentLogger::Log(const std::string& level, const std::string& message,
                      const LogContext& ctx) {
  const int64_t now = NowMs();
  const std::string line =
      FormatTs(now) + " level=" + level + " agent_id=" + agent_id_ +
      " request_id=" + (ctx.request_id.empty() ? "-" : ctx.request_id) +
      " task_id=" + (ctx.task_id.empty() ? "-" : ctx.task_id) +
      " trace_id=" + (ctx.trace_id.empty() ? "-" : ctx.trace_id) +
      " msg=\"" + Escape(message) + "\"";

  std::lock_guard<std::mutex> lock(mu_);
  if (out_.is_open()) {
    out_ << line << '\n';
    out_.flush();
  }
  if (also_stderr_) {
    std::cerr << line << '\n';
  }
}

}  // namespace maigent
