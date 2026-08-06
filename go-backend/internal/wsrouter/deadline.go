package wsrouter

import "time"

func websocketDeadline() time.Time {
	return time.Now().Add(time.Second)
}
