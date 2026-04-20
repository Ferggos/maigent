#pragma once

#include <string>

namespace maigent {

inline constexpr const char* kProtocolVersion = "v2";

inline constexpr const char* kSubjectCmdTaskSubmit = "msg.command.task.submit";
inline constexpr const char* kSubjectCmdTaskStatus = "msg.command.task.status";
inline constexpr const char* kSubjectCmdSystemStatus = "msg.command.system.status";
inline constexpr const char* kSubjectSvcDirSnapshotRequest =
    "msg.service.directory.snapshot.request";
inline constexpr const char* kSubjectCmdLeaseReserve = "msg.command.lease.reserve";
inline constexpr const char* kSubjectCmdLeaseCommit = "msg.command.lease.commit";
inline constexpr const char* kSubjectCmdLeaseRelease = "msg.command.lease.release";
inline constexpr const char* kSubjectCmdTaskExecLaunch = "msg.command.taskexec.launch";
inline constexpr const char* kSubjectCmdActuatorApply = "msg.command.actuator.apply";

inline constexpr const char* kSubjectEvtTaskSubmitted = "msg.event.task.submitted";
inline constexpr const char* kSubjectEvtTaskStarted = "msg.event.task.started";
inline constexpr const char* kSubjectEvtTaskFinished = "msg.event.task.finished";
inline constexpr const char* kSubjectEvtTaskFailed = "msg.event.task.failed";
inline constexpr const char* kSubjectEvtTaskQueued = "msg.event.task.queued";
inline constexpr const char* kSubjectEvtTaskDequeued = "msg.event.task.dequeued";
inline constexpr const char* kSubjectEvtTaskQueueTimeout = "msg.event.task.queue_timeout";
inline constexpr const char* kSubjectEvtControlApplied = "msg.event.control.applied";
inline constexpr const char* kSubjectEvtControlFailed = "msg.event.control.failed";
inline constexpr const char* kSubjectEvtActuatorApplied = "msg.event.actuator.applied";
inline constexpr const char* kSubjectEvtActuatorFailed = "msg.event.actuator.failed";
inline constexpr const char* kSubjectEvtLeaseDenied = "msg.event.lease.denied";
inline constexpr const char* kSubjectEvtLeaseExpired = "msg.event.lease.expired";
inline constexpr const char* kSubjectSvcBenchStart = "msg.service.bench.start";
inline constexpr const char* kSubjectSvcBenchStop = "msg.service.bench.stop";

inline constexpr const char* kSubjectAgentRegister = "msg.service.agent.register";
inline constexpr const char* kSubjectAgentHeartbeat = "msg.service.agent.heartbeat";
inline constexpr const char* kSubjectAgentGoodbye = "msg.service.agent.goodbye";

inline constexpr const char* kSubjectStatePressure = "msg.state.pressure";
inline constexpr const char* kSubjectStateForecast = "msg.state.forecast";
inline constexpr const char* kSubjectStateCapacity = "msg.state.capacity";
inline constexpr const char* kSubjectStateTargets = "msg.state.targets";

inline constexpr const char* kTaskExecQueueGroup = "taskexec";

inline std::string TaskExecControlSubject(const std::string& executor_id) {
  return "msg.command.taskexec.control." + executor_id;
}

}  // namespace maigent
