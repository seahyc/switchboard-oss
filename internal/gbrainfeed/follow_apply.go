package gbrainfeed

import "context"

// FollowToApply is one classified IG/social follow ready to apply to the graph: the
// platform identity (pk + handle + name), the profile signals the classifier reads,
// and — for a friend that matches an existing person — the canonical ID to merge the
// follow identity INTO (empty → stands as its own new node). The (handle→target)
// pairing is the human-reviewed merge set; this struct just carries it to the apply.
type FollowToApply struct {
	Platform    string // "instagram"
	UserID      string // IG numeric pk
	Handle      string // username, no '@'
	Name        string
	Signals     FollowSignals
	MergeTarget string // canonical ID to absorb this follow into (friends w/ an existing person); "" otherwise
}

// FollowApplyResult is one audit row of what the apply did — the trail the human gate
// (#49 / #17 provenance) requires.
type FollowApplyResult struct {
	Handle     string `json:"handle"`
	Name       string `json:"name"`
	NodeType   string `json:"node_type"` // person | entity
	Category   string `json:"category"`
	Action     string `json:"action"`      // merged | new-person-node | entity-node | unresolved
	SurvivorID string `json:"survivor_id"` // canonical that now carries the identity
	AbsorbedID string `json:"absorbed_id,omitempty"`
}

// ApplyFollowClassifications applies the (already human-reviewed) #49 classification to
// the graph, ONE-SHOT + durable: for each follow it (1) classifies person-vs-entity
// deterministically, (2) resolves the follow's IG identity to its canonical (must
// already be ingested — see the social ingestion step), (3) for a friend WITH a merge
// target, routes through ApproveMerge (the durable approve path that ForceMerges +
// persists the approved record + records merged_into provenance, #17) and tags the
// SURVIVOR's node type; otherwise tags the follow's own node (new person or entity).
// Returns an audit row per follow. Unresolved follows (identity not yet in graph) are
// reported, not failed — the caller re-runs after ingestion.
//
// Node-type durability across reprojection is the projection layer's concern (either
// gbrain persists node_type, or the social-author projection re-derives it via
// ClassifyFollow); the MERGES here are durable on their own via ApproveMerge.
func (p *Projector) ApplyFollowClassifications(ctx context.Context, follows []FollowToApply) ([]FollowApplyResult, error) {
	out := make([]FollowApplyResult, 0, len(follows))
	for _, f := range follows {
		cls := ClassifyFollow(f.Signals)
		res := FollowApplyResult{Handle: f.Handle, Name: f.Name, NodeType: cls.Type, Category: cls.Category}

		card, ok := p.ResolveSocialAuthor(f.Platform, f.UserID, f.Handle)
		if !ok {
			res.Action = "unresolved" // identity not ingested yet → re-run after social ingestion
			out = append(out, res)
			continue
		}
		igCanon := card.CanonicalID

		if cls.Type == "person" && f.MergeTarget != "" {
			mo, err := p.ApproveMerge(ctx, f.MergeTarget, igCanon)
			if err != nil {
				return out, err
			}
			survivor := mo.SurvivorID
			if survivor == "" {
				survivor = f.MergeTarget
			}
			p.resolver.SetNodeType(survivor, cls.Type, cls.Category)
			res.Action, res.SurvivorID, res.AbsorbedID = "merged", survivor, igCanon
		} else {
			p.resolver.SetNodeType(igCanon, cls.Type, cls.Category)
			res.SurvivorID = igCanon
			if cls.Type == "person" {
				res.Action = "new-person-node"
			} else {
				res.Action = "entity-node"
			}
		}
		out = append(out, res)
	}
	return out, nil
}
