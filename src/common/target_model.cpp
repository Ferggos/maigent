#include "maigent/common/target_model.h"

namespace maigent {

TargetKind InferTargetKind(TargetSource source, const std::string& cgroup_path,
                           int pid) {
  switch (source) {
    case TargetSource::kManagedTask:
      return TargetKind::kTask;
    case TargetSource::kExternalGroup:
      return TargetKind::kCgroup;
    case TargetSource::kExternalProcess:
      return TargetKind::kProcess;
    case TargetSource::kSystemService:
      return TargetKind::kSystem;
    case TargetSource::kUnspecified:
    default:
      break;
  }

  if (!cgroup_path.empty()) {
    return TargetKind::kCgroup;
  }
  if (pid > 0) {
    return TargetKind::kProcess;
  }
  return TargetKind::kUnspecified;
}

}  // namespace maigent
