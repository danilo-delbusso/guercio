package models

import "time"

type ActivityType string

const (
	ActivityPost   ActivityType = "post"
	ActivityLike   ActivityType = "like"
	ActivityRepost ActivityType = "repost"
)

// Activity represents a generic platform-agnostic interaction or post.
type Activity struct {
	Type ActivityType

	// The user performing the activity
	AccountID string

	// For ActivityPost
	PostID    string
	Text      string
	ReplyToID string // Optional parent post ID

	// For ActivityLike and ActivityRepost
	TargetID string // The post being liked or reposted

	CreatedAt time.Time
}
