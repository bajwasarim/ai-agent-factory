"""Smoke test for Serper tool."""

from core.tools.serper_tool import serper_search

res = serper_search(
    query="dentist in New York",
    num_results=3
)

print(f"Type: {type(res)}")
print(f"Organic results count: {len(res.get('organic', []))}")
print(f"Results: {res}")
