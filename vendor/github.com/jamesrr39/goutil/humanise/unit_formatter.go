package humanise

import (
	"fmt"
	"math"
)

var levelsText = []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"}

func HumaniseBytes(value int64) string {
	valueF64 := float64(value)
	for levels := 0; levels < len(levelsText); levels++ {
		oneOfThisLevel := math.Pow(1024, float64(levels))
		oneOfNextLevel := oneOfThisLevel * 1024
		if valueF64 >= oneOfNextLevel {
			continue
		}
		decimal := float64(valueF64) / float64(oneOfThisLevel)
		return fmt.Sprintf("%.1f %s", decimal, levelsText[levels])
	}
	// will never happen, because to get here `value` must be far bigger than int64 will handle
	return fmt.Sprintf("%d B", value)
}
