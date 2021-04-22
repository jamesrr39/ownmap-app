package ownmaprenderer

import (
	"image"
	"image/color"
	"image/draw"
)

func NewImageWithBackground(r image.Rectangle, c color.Color) *image.RGBA {
	img := image.NewRGBA(r)

	draw.Draw(img, img.Bounds(), image.NewUniform(c), image.ZP, draw.Src)

	return img
}
