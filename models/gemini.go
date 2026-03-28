package models

import (
	"context"
	"log"

	"google.golang.org/genai"
)

type GeminiClient struct {
	client    *genai.Client
	modelName string
}

func NewGeminiClient(ctx context.Context, modelName string) *GeminiClient {
	client, err := genai.NewClient(ctx, nil)
	if err != nil {
		log.Fatalf("Failed to initialize Gemini client: %w", err)
	}
	return &GeminiClient{
		client: client,
	}
}

func (g *GeminiClient) Query(ctx context.Context, prompt string) (string, error) {
	result, err := g.client.Models.GenerateContent(
		ctx,
		g.modelName,
		genai.Text(prompt),
		nil,
	)
	if err != nil {
		log.Printf("Failed to Query Gemini model: %w", err)
		return "", nil
	}
	return result.Text(), nil
}
