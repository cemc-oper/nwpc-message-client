package common

type EventStatus int

const (
	Unknown   EventStatus = 0
	Complete  EventStatus = 1
	Queued    EventStatus = 2
	Aborted   EventStatus = 3
	Submitted EventStatus = 4
	Active    EventStatus = 5
	Suspended EventStatus = 6
)

var statusList = [7]string{
	"unknown",
	"complete",
	"queued",
	"aborted",
	"submitted",
	"active",
	"suspended",
}

func (status EventStatus) String() string {
	if status < Unknown || status > Suspended {
		return statusList[0]
	}

	return statusList[status]
}

func ToEventStatus(status string) EventStatus {
	for index, s := range statusList {
		if s == status {
			return EventStatus(index)
		}
	}
	return Unknown
}
