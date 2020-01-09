package entry

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/pkg/errors"
)

func SortByClocks(a, b *Entry, resolveConflict func(a *Entry, b *Entry) (int, error)) (int, error) {
	diff := a.GetClock().Compare(b.GetClock())

	if diff == 0 {
		return resolveConflict(a, b)
	}

	return diff, nil
}

func SortByClockID(a, b *Entry, resolveConflict func(a *Entry, b *Entry) (int, error)) (int, error) {
	comparedIDs := bytes.Compare(a.GetClock().GetID(), b.GetClock().GetID())

	if comparedIDs == 0 {
		return resolveConflict(a, b)
	}
	if comparedIDs < 0 {
		return -1, nil
	}

	return 1, nil
}

func SortFirstWriteWins(a, b *Entry) (int, error) {
	res, err := LastWriteWins(a, b)

	return res * -1, err
}

func SortLastWriteWins(a, b *Entry) (int, error) {
	sortFirst := func(_, _ *Entry) (int, error) {
		return 1, nil
	}
	sortByID := func(a, b *Entry) (int, error) {
		return SortByClockID(a, b, sortFirst)
	}
	sortByEntryClocks := func(a, b *Entry) (int, error) {
		return SortByClocks(a, b, sortByID)
	}

	return sortByEntryClocks(a, b)
}

func SortNoZeroes(compFunc func(a, b *Entry) (int, error)) func(a, b *Entry) (int, error) {
	return func(a, b *Entry) (int, error) {
		ret, err := compFunc(a, b)
		if ret != 0 || err != nil {
			return ret, err
		}

		return 0, errors.New(`err: Your log's tiebreaker function has returned zero and therefore cannot be`)
	}
}

func Reverse(a []*Entry) {
	for i := len(a)/2 - 1; i >= 0; i-- {
		opp := len(a) - 1 - i
		a[i], a[opp] = a[opp], a[i]
	}
}

func Sort(compFunc func(a, b *Entry) (int, error), values []*Entry) {
	sort.SliceStable(values, func(i, j int) bool {
		ret, err := compFunc(values[i], values[j])
		if err != nil {
			fmt.Printf("error while comparing: %v\n", err)
			return false
		}
		return ret < 0
	})
}
