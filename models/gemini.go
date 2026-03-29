package models

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"google.golang.org/genai"
)

type GeminiClient struct {
	client    *genai.Client
	modelName string
	functions []*genai.FunctionDeclaration
}

func NewGeminiClient(ctx context.Context, modelName string) *GeminiClient {
	client, err := genai.NewClient(ctx, nil)
	if err != nil {
		log.Fatalf("Failed to initialize Gemini client: %w", err)
	}
	return &GeminiClient{
		client:    client,
		modelName: modelName,
		functions: []*genai.FunctionDeclaration{},
	}
}

func (g *GeminiClient) LoadTool(filepath string) {
	data, err := os.ReadFile(filepath)
	if err != nil {
		log.Fatalf("Failed to read function declaration file: %w", err)
	}

	var function genai.FunctionDeclaration
	err = json.Unmarshal(data, &function)
	if err != nil {
		log.Fatalf("Failed to unmarshal function declaration JSON: %w", err)
	}
	g.functions = append(g.functions, &function)
}

func (g *GeminiClient) LoadTools(dirPath string) {
	files, _ := os.ReadDir(dirPath)

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if filepath.Ext(file.Name()) != ".json" {
			continue
		}
		fullPath := filepath.Join(dirPath, file.Name())
		g.LoadTool(fullPath)
	}
}

func (g *GeminiClient) Query(ctx context.Context, prompt string) (string, error) {
	config := &genai.GenerateContentConfig{
		Tools: []*genai.Tool{
			{FunctionDeclarations: g.functions},
		},
	}
	log.Printf("Config: %v\n", config)

	response, err := g.client.Models.GenerateContent(ctx, g.modelName, genai.Text(prompt), config)
	if err != nil {
		log.Printf("Failed to Query Gemini model: %w", err)
		return "", nil
	}

	for _, part := range response.Candidates[0].Content.Parts {
		if part.FunctionCall != nil {
			function := part.FunctionCall
			return fmt.Sprintf("Function call: %s(args: %v)", function.Name, function.Args), nil
		}

		if part.Text != "" {
			return part.Text, nil
		}
	}

	return "", fmt.Errorf("Unknown response returned: %v", response)
}
