package testutil

import (
	nanoid "github.com/matoous/go-nanoid"
)

// RandomName returns a random "name" of 10 characters.
func RandomName() string {
	return nanoid.MustGenerate("abcdefghijklmnopqrstuvwxyz", 10)
}
