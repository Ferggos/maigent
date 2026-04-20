#pragma once

#include <fstream>
#include <mutex>
#include <string>

namespace maigent {

struct LogContext {
  std::string request_id;
  std::string task_id;
  std::string trace_id;
};

class AgentLogger {
 public:
  AgentLogger(std::string agent_id, std::string file_path, bool also_stderr = true);

  void Info(const std::string& message, const LogContext& ctx = {});
  void Warn(const std::string& message, const LogContext& ctx = {});
  void Error(const std::string& message, const LogContext& ctx = {});
  void Debug(const std::string& message, const LogContext& ctx = {});

  const std::string& agent_id() const { return agent_id_; }

 private:
  void Log(const std::string& level, const std::string& message, const LogContext& ctx);

  std::string agent_id_;
  std::string file_path_;
  bool also_stderr_;
  std::mutex mu_;
  std::ofstream out_;
};

}  // namespace maigent
