package models

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"google.golang.org/genai"
)

type GeminiClient struct {
	client             *genai.Client
	modelName          string
	functions          []*genai.FunctionDeclaration
	functionHandlerUrl string
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

func (g *GeminiClient) HandleFunctionCall(ctx context.Context, function *genai.FunctionCall) (any, error) {
	jsonBody, err := json.Marshal(function.Args)
	if err != nil {
		return nil, err
	}

	url := g.functionHandlerUrl + "/" + function.Name

	req, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}

func (g *GeminiClient) Query(ctx context.Context, prompt string) (string, error) {
	config := &genai.GenerateContentConfig{
		Tools: []*genai.Tool{
			{FunctionDeclarations: g.functions},
		},
	}

	response, err := g.client.Models.GenerateContent(ctx, g.modelName, genai.Text(prompt), config)
	if err != nil {
		log.Printf("Failed to Query Gemini model: %w", err)
		return "", nil
	}

	for _, part := range response.Candidates[0].Content.Parts {
		if part.FunctionCall != nil {
			function := part.FunctionCall
			result, err := g.HandleFunctionCall(ctx, function)
			if err != nil {
				return "", err
			}
			resultBytes, err := json.Marshal(result)
			return string(resultBytes), err
		}

		if part.Text != "" {
			return part.Text, nil
		}
	}

	return "", fmt.Errorf("Unknown response returned: %v", response)
}
