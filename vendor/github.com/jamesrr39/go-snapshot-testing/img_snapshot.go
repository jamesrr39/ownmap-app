package snapshot

import (
	"bufio"
	"bytes"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/jpeg"
	"image/png"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/jamesrr39/goutil/open"
)

type cellType struct {
	Color color.Color
}

type rowType struct {
	Cells []*cellType
}

type imageSnapshotType struct {
	Rows []*rowType
}

const (
	CI_MODE_ENV_VAR = "CI_MODE"
)

func NewImageSnapshot(img image.Image) *SnapshotType {
	value := marshalImageSnapshot(img)

	return &SnapshotType{
		DataType: SnapshotDataTypeImage,
		Value:    value,
		OnBadMatch: func(t *testing.T, expected string) error {
			openComparision, ok := os.LookupEnv(CI_MODE_ENV_VAR)
			if ok && openComparision == "1" {
				t.Logf("preview not automatically opened as you are in CI mode. To leave CI mode, unset %s, or set it to 0.", CI_MODE_ENV_VAR)
				return nil
			}

			// location, err := drawImageComparison(expected, img)
			tempdir, err := ioutil.TempDir("", "go-snapshot")
			if err != nil {
				return err
			}

			actualLocation := filepath.Join(tempdir, "actual.png")
			expectedLocation := filepath.Join(tempdir, "expected.png")

			actualF, err := os.Create(actualLocation)
			if err != nil {
				return err
			}
			defer actualF.Close()

			err = png.Encode(actualF, img)
			if err != nil {
				return err
			}

			expectedF, err := os.Create(expectedLocation)
			if err != nil {
				return err
			}
			defer expectedF.Close()

			expectedImg, err := unmarshalImgSnapshot(bytes.NewBufferString(expected))
			if err != nil {
				return err
			}

			err = png.Encode(expectedF, expectedImg)
			if err != nil {
				return err
			}

			t.Logf("bad match found. Comparision can be viewed here: %s\n", tempdir)

			open.OpenURL(expectedLocation)
			open.OpenURL(actualLocation)

			return nil
		},
	}
}

type rowCell struct {
	ColorString string
	Count       int
}

func rowCellsToString(rowCells []*rowCell) string {
	var s []string
	for _, rc := range rowCells {
		s = append(s, fmt.Sprintf("%d %s", rc.Count, rc.ColorString))
	}
	return strings.Join(s, "|")
}

func marshalImageSnapshot(img image.Image) string {
	// convert to series of rows and cells
	snapshot := new(imageSnapshotType)
	bounds := img.Bounds()
	for y := 0; y < bounds.Max.Y; y++ {
		row := new(rowType)
		for x := 0; x < bounds.Max.X; x++ {
			cell := &cellType{
				Color: img.At(x, y),
			}
			row.Cells = append(row.Cells, cell)
		}
		snapshot.Rows = append(snapshot.Rows, row)
	}

	// marshal the rows and cells
	var rowStrings []string
	for _, row := range snapshot.Rows {
		rowCells := []*rowCell{}
		var lastColor string
		cellsOfColorInARow := 0

		for idx, cell := range row.Cells {
			thisColorString := colorToString(cell.Color)
			if lastColor != thisColorString {
				if cellsOfColorInARow != 0 {
					rowCells = append(rowCells, &rowCell{
						ColorString: lastColor,
						Count:       cellsOfColorInARow,
					})
				}
				cellsOfColorInARow = 1
				lastColor = thisColorString
			} else {
				cellsOfColorInARow++
				if idx == len(row.Cells)-1 {
					// last cell
					if cellsOfColorInARow != 0 {
						rowCells = append(rowCells, &rowCell{
							ColorString: lastColor,
							Count:       cellsOfColorInARow,
						})
					}
				}
			}
		}

		rowStrings = append(rowStrings, rowCellsToString(rowCells))
	}
	return strings.Join(rowStrings, "\n")
}

func (snapshot *imageSnapshotType) toImage() image.Image {
	xSize := 0
	for _, row := range snapshot.Rows {
		if len(row.Cells) > xSize {
			xSize = len(row.Cells)
		}
	}

	img := image.NewRGBA(image.Rect(0, 0, xSize, len(snapshot.Rows)))

	for rowIndex, row := range snapshot.Rows {
		for cellIndex, cell := range row.Cells {
			img.Set(cellIndex, rowIndex, cell.Color)
		}
	}

	return img
}

func colorToString(color color.Color) string {
	r, g, b, a := color.RGBA()
	return fmt.Sprintf("%d,%d,%d,%d", r, g, b, a)
}

func unmarshalImgSnapshot(reader io.Reader) (image.Image, error) {
	imgSnapshot := new(imageSnapshotType)

	buf := bufio.NewScanner(reader)
	for buf.Scan() {
		row := new(rowType)
		line := buf.Text()
		if line == "" {
			continue
		}
		fragments := strings.Split(line, "|")
		for _, fragment := range fragments {
			cellFragments := strings.Split(fragment, " ")
			count, err := strconv.ParseUint(cellFragments[0], 10, 64)
			if err != nil {
				return nil, err
			}
			colorFragments := strings.Split(cellFragments[1], ",")

			r, g, b, a, err := colorFragmentsToColors(colorFragments)
			if err != nil {
				return nil, err
			}

			for i := uint64(0); i < count; i++ {
				row.Cells = append(row.Cells, &cellType{
					color.RGBA{
						R: r,
						G: g,
						B: b,
						A: a,
					},
				})
			}
		}
		imgSnapshot.Rows = append(imgSnapshot.Rows, row)
	}

	if buf.Err() != nil {
		return nil, buf.Err()
	}

	return imgSnapshot.toImage(), nil
}

func colorFragmentsToColors(fragments []string) (r, g, b, a uint8, err error) {
	uints := make([]uint8, 4)
	for index, fragment := range fragments {
		val, err := strconv.ParseUint(fragment, 10, 64)
		if err != nil {
			return 0, 0, 0, 0, err
		}
		uints[index] = uint8(val)
	}
	return uints[0], uints[1], uints[2], uints[3], nil
}

// drawImageComparision draws a comparision and returns the filepath of the comparision image
func drawImageComparison(expected string, actual image.Image) (string, error) {
	expectedImg, err := unmarshalImgSnapshot(bytes.NewBufferString(expected))
	if err != nil {
		return "", fmt.Errorf("couldn't build a snapshot image from the string provided. Error: %q.\nSnapshot string: %s", err, expected)
	}

	tmpSnapshotComparisionDir := "/tmp/go-snapshot"

	err = os.MkdirAll(tmpSnapshotComparisionDir, 0755)
	if err != nil {
		return "", fmt.Errorf("failed to make snapshot comparision dir at %q. Error: %q", tmpSnapshotComparisionDir, err)
	}

	tempFile, err := ioutil.TempFile(tmpSnapshotComparisionDir, "go-snapshot*.jpg")
	if err != nil {
		return "", fmt.Errorf("failed to make snapshot comparision image. Error: %q", err)
	}
	defer tempFile.Close()

	xMiddleMarginPx := 10

	joinedX := expectedImg.Bounds().Max.X + xMiddleMarginPx + actual.Bounds().Max.X
	joinedY := int(math.Max(float64(expectedImg.Bounds().Max.Y), float64(actual.Bounds().Max.Y)))

	joinedImg := image.NewRGBA(image.Rect(0, 0, joinedX, joinedY))
	grey := color.RGBA{R: 0xc1, G: 0xb6, B: 0xc0}

	// base color
	draw.Draw(joinedImg, joinedImg.Bounds(), image.NewUniform(grey), image.ZP, draw.Src)

	// draw on expected
	draw.Draw(
		joinedImg,
		expectedImg.Bounds(),
		// image.Rect(5, 0, 15, 10),
		expectedImg,
		image.ZP,
		draw.Src,
	)

	// draw on actual
	draw.Draw(
		joinedImg,
		// joinedImg.Bounds(),
		image.Rect(expectedImg.Bounds().Max.X+xMiddleMarginPx, 0, joinedX, joinedY),
		actual,
		image.ZP,
		// image.Point{X: expectedImg.Bounds().Max.X + xMiddleMarginPx - 1},
		draw.Src,
	)

	err = jpeg.Encode(tempFile, joinedImg, nil)
	if err != nil {
		return "", fmt.Errorf("failed to write the comparision image. Error: %q", err)
	}

	return tempFile.Name(), nil
}
