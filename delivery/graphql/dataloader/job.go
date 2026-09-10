package _dataloader

import (
	"context"

	"github.com/graph-gophers/dataloader/v6"
)

// JobBatchFunc resolves many job ids in one repository call, collapsing N lock
// acquisitions into one. Results are returned positionally: results[i] answers keys[i].
//
// A missing id is not an error — it resolves to nil, which the resolver renders as a
// null Job.
func (s GeneralDataloader) JobBatchFunc(ctx context.Context, keys dataloader.Keys) []*dataloader.Result {
	ids := keys.Keys()
	results := make([]*dataloader.Result, len(ids))

	found, err := s.jobRepo.FindByIDs(ctx, ids)
	for i, id := range ids {
		switch {
		case err != nil:
			results[i] = &dataloader.Result{Error: err}
		case found[id] != nil:
			results[i] = &dataloader.Result{Data: found[id]}
		default:
			results[i] = &dataloader.Result{Data: nil}
		}
	}

	return results
}
