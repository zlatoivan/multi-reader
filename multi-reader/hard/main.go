package main

func main() {
	tests := append(testCases, privateTestCases...)

	for _, tc := range tests {
		name := tc.name
		run := tc.run

		CustomTestBody(
			name,
			func() struct{} {
				return struct{}{}
			},
			func(_ struct{}) bool {
				return run()
			},
		)
	}
}
