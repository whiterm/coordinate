package leader

// ActionType is the type of action
type ActionType string

// block of constants for action types
const (
	ActionTypeCreate = "create"
	ActionTypeUpdate = "update"
	ActionTypeDelete = "delete"
)

// Action is a change of node in the etcd
type Action struct {
	// Type is the name of the operation that occurred.
	Type ActionType `json:"action"`

	// Key represents the unique location of this Action (e.g. "/foo/bar").
	Key string `json:"key"`

	// Value is the current data stored on this Action. If this action is a delete, the value will be empty.
	Value string `json:"value"`
}
