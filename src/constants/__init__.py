import enum

class SeverityLevel(str, enum.Enum):
    CRITICAL = "critical"
    HIGH     = "high"
    MEDIUM   = "medium"
    LOW      = "low"


class PriorityLevel(str, enum.Enum):
    P0 = "P0"
    P1 = "P1"
    P2 = "P2"
    P3 = "P3"

class TicketStatus(str, enum.Enum):
    NEW          = "new"
    ACKNOWLEDGED = "acknowledged"
    IN_PROGRESS  = "in_progress"
    ON_HOLD      = "on_hold"
    RESOLVED     = "resolved"
    CLOSED       = "closed"
    REOPENED     = "reopened"

class TicketSource(str, enum.Enum):
    PORTAL = "portal"
    EMAIL  = "email"


class Environment(str, enum.Enum):
    PROD    = "prod"
    STAGING = "stage"
    DEV     = "dev"


class EmailProcessingStatus(str, enum.Enum):
    PENDING    = "pending"
    PROCESSING = "processing"
    PROCESSED  = "processed"
    DISCARDED  = "discarded"
    FAILED     = "failed"


class NotificationChannel(str, enum.Enum):
    EMAIL  = "email"
    IN_APP = "in_app"


class NotificationStatus(str, enum.Enum):
    PENDING = "pending"
    SENT    = "sent"
    FAILED  = "failed"
    READ    = "read"


class ConversationAuthorType(str, enum.Enum):
    AGENT    = "agent"
    CUSTOMER = "customer"


class TierName(str, enum.Enum):
    STARTER    = "starter"
    STANDARD   = "standard"
    ENTERPRISE = "enterprise"   