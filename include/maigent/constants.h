#pragma once

#include <string>

namespace maigent {

inline constexpr const char* kProtocolVersion = "v1";

inline constexpr const char* kSubjectCmdTaskSubmit = "cmd.task.submit";
inline constexpr const char* kSubjectCmdLeaseReserve = "cmd.lease.reserve";
inline constexpr const char* kSubjectCmdLeaseCommit = "cmd.lease.commit";
inline constexpr const char* kSubjectCmdLeaseRelease = "cmd.lease.release";
inline constexpr const char* kSubjectCmdExecLaunch = "cmd.exec.launch";
inline constexpr const char* kSubjectCmdExecControl = "cmd.exec.control";
inline constexpr const char* kSubjectCmdDirSnapshot = "cmd.dir.snapshot";
inline constexpr const char* kSubjectCmdSystemStatus = "cmd.system.status";

inline constexpr const char* kSubjectEvtTaskSubmitted = "evt.task.submitted";
inline constexpr const char* kSubjectEvtTaskStarted = "evt.task.started";
inline constexpr const char* kSubjectEvtTaskFinished = "evt.task.finished";
inline constexpr const char* kSubjectEvtTaskFailed = "evt.task.failed";
inline constexpr const char* kSubjectEvtLeaseDenied = "evt.lease.denied";
inline constexpr const char* kSubjectEvtControlApplied = "evt.control.applied";
inline constexpr const char* kSubjectEvtBenchStart = "evt.bench.start";
inline constexpr const char* kSubjectEvtBenchStop = "evt.bench.stop";

inline constexpr const char* kSubjectAgentRegister = "agent.register";
inline constexpr const char* kSubjectAgentHeartbeat = "agent.heartbeat";
inline constexpr const char* kSubjectAgentGoodbye = "agent.goodbye";

inline constexpr const char* kSubjectStatePressure = "state.pressure";
inline constexpr const char* kSubjectStateForecast = "state.forecast";
inline constexpr const char* kSubjectStateCapacity = "state.capacity";

inline constexpr const char* kExecQueueGroup = "exec";

}  // namespace maigent
