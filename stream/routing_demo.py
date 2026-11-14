
# D1 - RabbitMQ Topic Routing Demonstration

# Binding patterns from the Part D routing exercise
bindings = {
    "Ravi": ["bangalore.koramangala.food"],
    "Meena": ["bangalore.koramangala.food"],
    "Priya": ["bangalore.indiranagar.ride"],
    "Arjun": ["bangalore.*.ride"],
    "Karthik": ["chennai.tnagar.food"],
    "Control": ["bangalore.#"]
}

routing_keys = [
    "bangalore.koramangala.food",
    "bangalore.indiranagar.ride",
    "bangalore.whitefield.ride",
    "chennai.tnagar.food",
    "chennai.whitefield.ride",
    "bangalore.koramangala"
]

def topic_matches(pattern, routing_key):
    pattern_parts = pattern.split(".")
    key_parts = routing_key.split(".")

    def match(p, k):
        if not p:
            return not k

        if p[0] == "#":
            if len(p) == 1:
                return True
            return any(
                match(p[1:], k[i:])
                for i in range(len(k) + 1)
            )

        if not k:
            return False

        if p[0] == "*" or p[0] == k[0]:
            return match(p[1:], k[1:])

        return False

    return match(pattern_parts, key_parts)


print("D1 - RABBITMQ TOPIC ROUTING")
print("=" * 65)

for key in routing_keys:
    receivers = []

    for queue, patterns in bindings.items():
        if any(topic_matches(pattern, key) for pattern in patterns):
            receivers.append(queue)

    print(f"\nRouting key: {key}")
    print("Receiving queues:", ", ".join(receivers) if receivers else "Dropped")

print("\nD1 routing demonstration completed.")

