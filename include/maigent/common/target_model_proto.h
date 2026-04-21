#pragma once

#include "maigent.pb.h"
#include "maigent/common/target_model.h"

namespace maigent {

TargetSource FromProtoTargetSourceType(TargetSourceType source_type);
TargetSourceType ToProtoTargetSourceType(TargetSource source);

TargetKind FromProtoTargetType(TargetType target_type);
TargetType ToProtoTargetType(TargetKind kind);

TargetAction FromProtoControlActionType(ControlActionType action_type);
ControlActionType ToProtoControlActionType(TargetAction action);

UnifiedTarget TargetFromProto(const TargetInfo& proto_target);
TargetInfo ToProtoTargetInfo(const UnifiedTarget& target);

}  // namespace maigent
