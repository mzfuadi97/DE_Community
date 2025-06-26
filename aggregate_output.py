import json
from collections import Counter, defaultdict

# Baca data hasil join
with open('output_data.json') as f:
    data = json.load(f)

# 1. Frekuensi Permintaan Per Pengguna
freq_per_user = Counter()
for item in data:
    user_id = item.get('user_id')
    if user_id:
        freq_per_user[user_id] += 1

with open('freq_per_user.json', 'w') as f:
    json.dump(freq_per_user, f, indent=2)

# 2. Performa API (rata-rata response_time per endpoint)
api_perf = defaultdict(list)
for item in data:
    endpoint = item.get('endpoint')
    resp_time = item.get('response_time')
    if endpoint and resp_time is not None:
        api_perf[endpoint].append(resp_time)

api_perf_avg = {ep: sum(times)/len(times) for ep, times in api_perf.items() if times}

with open('api_performance.json', 'w') as f:
    json.dump(api_perf_avg, f, indent=2)

# 3. Frekuensi Penggunaan Endpoint
freq_endpoint = Counter()
for item in data:
    endpoint = item.get('endpoint')
    if endpoint:
        freq_endpoint[endpoint] += 1

with open('freq_endpoint.json', 'w') as f:
    json.dump(freq_endpoint, f, indent=2) 