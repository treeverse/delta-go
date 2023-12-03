package action

type Format struct {
	Proviver string            `json:"provider"`
	Options  map[string]string `json:"options"`
}
