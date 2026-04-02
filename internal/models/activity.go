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
	// Platform-specific URL to the user's profile
	AccountURL string

	// For ActivityPost
	PostID  string
	PostURL string
	Text    string
	// Optional parent post ID
	ReplyToID       string
	ReplyToURL      string
	ReplyToAuthorID string

	// For ActivityLike and ActivityRepost
	TargetID       string // The post being liked or reposted
	TargetURL      string
	TargetAuthorID string

	CreatedAt time.Time
}
