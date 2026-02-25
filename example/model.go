package model

import (
	synapse "synapse/src/common"
)

const (
	UserReq_singapore                = "Plan me a trip to Singapore for 2 weeks during April with a budget of $5000"
	AttractionsAgentInput_singapore  = "Find attractions in Singapore for 2 weeks during April given a budget of $5000"
	AttractionsAgentOutput_singapore = `Here is a two-week Singapore plan for April with a total budget of about $5,000 USD.

Singapore is warm and humid in April (around 25–32°C) with occasional short rain showers. Lightweight clothing, comfortable walking shoes, and staying hydrated are important.

Major attractions to visit:

Marina Bay area: Walk around Marina Bay Sands, the waterfront promenade, and Merlion Park. Watch the free evening light and water shows. Visit the Marina Bay Sands SkyPark Observation Deck for skyline views.

Gardens by the Bay: Explore the outdoor gardens for free and purchase tickets for the Cloud Forest and Flower Dome. Stay for the Supertree Grove light show at night.

Sentosa Island: Spend at least two full days here. Visit Universal Studios Singapore for a theme park day. Relax at Siloso or Palawan Beach. Consider Skyline Luge, SEA Aquarium, or Adventure Cove Waterpark.

Singapore Zoo and Night Safari: Dedicate one full day to the zoo and another evening for Night Safari. These are world-class wildlife experiences.

Jurong Bird Park’s replacement, Bird Paradise (at Mandai): Optional half-day if you enjoy wildlife.

Singapore Botanic Gardens: Free UNESCO World Heritage site. Walk through the National Orchid Garden (small entrance fee).

MacRitchie Treetop Walk: Free hiking experience with a suspended bridge above the forest canopy.

Pulau Ubin: Rustic island day trip. Rent bicycles and explore nature trails.

Cultural neighborhoods:
Chinatown for temples, Buddha Tooth Relic Temple, and hawker food.
Little India for Sri Veeramakaliamman Temple and vibrant streets.
Kampong Glam for Sultan Mosque and Arab Street.

Clarke Quay and Boat Quay: Riverside dining and nightlife.

Orchard Road: Shopping district.

Hawker centres to try:
Maxwell Food Centre
Chinatown Complex Food Centre
Lau Pa Sat
Newton Food Centre
Tekka Centre

Sample 14-day structure:

Days 1–2: Marina Bay, Merlion, Gardens by the Bay, SkyPark.
Day 3: Chinatown and Little India.
Day 4: Kampong Glam and Haji Lane.
Day 5: Singapore Botanic Gardens and Orchard Road.
Day 6: Sentosa beaches and attractions.
Day 7: Universal Studios Singapore.
Day 8: Singapore Zoo.
Day 9: Night Safari.
Day 10: MacRitchie Treetop Walk.
Day 11: Pulau Ubin.
Day 12: Bird Paradise or shopping and relaxation.
Day 13: Hawker food exploration and Clarke Quay evening.
Day 14: Free day for revisiting favorites and last-minute shopping.

Approximate budget breakdown for one person (adjust if traveling with two):

Flights (from North America): $1,200–$1,800
Accommodation (mid-range hotel, 14 nights at $150/night): about $2,100
Food (average $35/day): about $490
Transport (MRT and bus, about $15/day): about $210
Attractions and tickets (theme park, zoo, observation deck, etc.): about $400–$600
Miscellaneous and shopping: $400–$600

Total estimated range: approximately $4,800–$5,000.

To stay within budget, use public transport (MRT is efficient and affordable), eat most meals at hawker centres, mix paid attractions with free parks and neighborhoods, and book major tickets in advance.`
)

type ModelResponse struct {
	ResponseType synapse.TaskStatus
	Response     string
}

type SupervisorModelResponse struct {
	Namespace string
	Prompt    []byte
}

func Query(prompt []byte, context []byte) []byte {
	if string(prompt) == AttractionsAgentInput_singapore {
		return []byte(AttractionsAgentOutput_singapore)
	}

	return []byte("")
}

func DistributeTasks(prompt []byte) []SupervisorModelResponse {
	return []SupervisorModelResponse{
		{
			Namespace: "TaskAllocator_AttractionsAgent",
			Prompt:    []byte(AttractionsAgentInput_singapore),
		},
	}
}

// implement supervisor agent
// try running
// implement flights agent
// try running
// add mocks
// add user input/output mock
// add other 2 agents
