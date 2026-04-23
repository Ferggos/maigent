#include "maigent/common/target_model_proto.h"

namespace maigent {

TargetSource FromProtoTargetSourceType(TargetSourceType source_type) {
  switch (source_type) {
    case MANAGED_TASK:
      return TargetSource::kManagedTask;
    case EXTERNAL_GROUP:
      return TargetSource::kExternalGroup;
    case EXTERNAL_PROCESS:
      return TargetSource::kExternalProcess;
    case SYSTEM_SERVICE:
      return TargetSource::kSystemService;
    case TARGET_SOURCE_UNSPECIFIED:
    default:
      return TargetSource::kUnspecified;
  }
}

TargetSourceType ToProtoTargetSourceType(TargetSource source) {
  switch (source) {
    case TargetSource::kManagedTask:
      return MANAGED_TASK;
    case TargetSource::kExternalGroup:
      return EXTERNAL_GROUP;
    case TargetSource::kExternalProcess:
      return EXTERNAL_PROCESS;
    case TargetSource::kSystemService:
      return SYSTEM_SERVICE;
    case TargetSource::kUnspecified:
    default:
      return TARGET_SOURCE_UNSPECIFIED;
  }
}

TargetKind FromProtoTargetType(TargetType target_type) {
  switch (target_type) {
    case TARGET_TASK:
      return TargetKind::kTask;
    case TARGET_PROCESS:
      return TargetKind::kProcess;
    case TARGET_CGROUP:
      return TargetKind::kCgroup;
    case TARGET_SYSTEM:
      return TargetKind::kSystem;
    case TARGET_TYPE_UNSPECIFIED:
    default:
      return TargetKind::kUnspecified;
  }
}

TargetType ToProtoTargetType(TargetKind kind) {
  switch (kind) {
    case TargetKind::kTask:
      return TARGET_TASK;
    case TargetKind::kProcess:
      return TARGET_PROCESS;
    case TargetKind::kCgroup:
      return TARGET_CGROUP;
    case TargetKind::kSystem:
      return TARGET_SYSTEM;
    case TargetKind::kUnspecified:
    default:
      return TARGET_TYPE_UNSPECIFIED;
  }
}

TargetAction FromProtoControlActionType(ControlActionType action_type) {
  switch (action_type) {
    case RENICE:
      return TargetAction::kRenice;
    case SET_CPU_WEIGHT:
      return TargetAction::kSetCpuWeight;
    case SET_CPU_MAX:
      return TargetAction::kSetCpuMax;
    case SET_MEM_HIGH:
      return TargetAction::kSetMemHigh;
    case SET_MEM_MAX:
      return TargetAction::kSetMemMax;
    case FREEZE:
      return TargetAction::kFreeze;
    case THAW:
      return TargetAction::kThaw;
    case KILL:
      return TargetAction::kKill;
    case CONTROL_ACTION_UNSPECIFIED:
    default:
      return TargetAction::kUnspecified;
  }
}

ControlActionType ToProtoControlActionType(TargetAction action) {
  switch (action) {
    case TargetAction::kRenice:
      return RENICE;
    case TargetAction::kSetCpuWeight:
      return SET_CPU_WEIGHT;
    case TargetAction::kSetCpuMax:
      return SET_CPU_MAX;
    case TargetAction::kSetMemHigh:
      return SET_MEM_HIGH;
    case TargetAction::kSetMemMax:
      return SET_MEM_MAX;
    case TargetAction::kFreeze:
      return FREEZE;
    case TargetAction::kThaw:
      return THAW;
    case TargetAction::kKill:
      return KILL;
    case TargetAction::kUnspecified:
    default:
      return CONTROL_ACTION_UNSPECIFIED;
  }
}

UnifiedTarget TargetFromProto(const TargetInfo& proto_target) {
  UnifiedTarget target;
  target.target_id = proto_target.target_id();
  target.source = FromProtoTargetSourceType(proto_target.source_type());
  target.kind = InferTargetKind(target.source, proto_target.cgroup_path(),
                                proto_target.pid());
  target.owner_executor_id = proto_target.owner_executor_id();
  target.task_id = proto_target.task_id();
  target.pid = proto_target.pid();
  target.cgroup_path = proto_target.cgroup_path();
  target.task_class = proto_target.task_class();
  target.priority = proto_target.priority();
  target.is_protected = proto_target.is_protected();
  target.allowed_actions.reserve(proto_target.allowed_actions_size());
  for (int i = 0; i < proto_target.allowed_actions_size(); ++i) {
    target.allowed_actions.push_back(
        FromProtoControlActionType(proto_target.allowed_actions(i)));
  }
  target.cpu_usage = proto_target.cpu_usage();
  target.memory_current_mb = proto_target.memory_current_mb();
  target.cpu_pressure = proto_target.cpu_pressure();
  target.memory_pressure = proto_target.memory_pressure();
  target.io_pressure = proto_target.io_pressure();
  target.cpu_intensity = proto_target.cpu_intensity();
  target.memory_delta_mb = proto_target.memory_delta_mb();
  target.memory_ratio_of_host = proto_target.memory_ratio_of_host();
  target.age_sec = proto_target.age_sec();
  target.cpu_throttled_ratio = proto_target.cpu_throttled_ratio();
  target.memory_events_high_delta = proto_target.memory_events_high_delta();
  target.memory_events_oom_delta = proto_target.memory_events_oom_delta();
  return target;
}

TargetInfo ToProtoTargetInfo(const UnifiedTarget& target) {
  TargetInfo out;
  out.set_target_id(target.target_id);
  out.set_source_type(ToProtoTargetSourceType(target.source));
  out.set_owner_executor_id(target.owner_executor_id);
  out.set_task_id(target.task_id);
  out.set_pid(target.pid);
  out.set_cgroup_path(target.cgroup_path);
  out.set_task_class(target.task_class);
  out.set_priority(target.priority);
  out.set_is_protected(target.is_protected);
  for (const auto action : target.allowed_actions) {
    out.add_allowed_actions(ToProtoControlActionType(action));
  }
  out.set_cpu_usage(target.cpu_usage);
  out.set_memory_current_mb(target.memory_current_mb);
  out.set_cpu_pressure(target.cpu_pressure);
  out.set_memory_pressure(target.memory_pressure);
  out.set_io_pressure(target.io_pressure);
  out.set_cpu_intensity(target.cpu_intensity);
  out.set_memory_delta_mb(target.memory_delta_mb);
  out.set_memory_ratio_of_host(target.memory_ratio_of_host);
  out.set_age_sec(target.age_sec);
  out.set_cpu_throttled_ratio(target.cpu_throttled_ratio);
  out.set_memory_events_high_delta(target.memory_events_high_delta);
  out.set_memory_events_oom_delta(target.memory_events_oom_delta);
  return out;
}

}  // namespace maigent
