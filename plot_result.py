import matplotlib.pyplot as plt

files = [
    ('res_one_node_opt.txt', '1 Node (Optimized)'),
    ('res_one_node.txt', '1 Node'),
    ('res_three_node_opt.txt', '3 Nodes (Optimized)'),
    ('res_three_node.txt', '3 Nodes'),
]

times = []
memories = []
labels = []

for file_name, label in files:
    with open(file_name, 'r') as f:
        lines = f.readlines()
        time = None
        memory = None
        for line in lines:
            if '[TIME]' in line:
                time = float(line.strip().split(']')[1])
            elif '[MEMORY]' in line:
                value = line.strip().split(']')[1].lower().replace('mb', '')
                memory = float(value.strip())
        if time is not None and memory is not None:
            times.append(time)
            memories.append(memory)
            labels.append(label)
        else:
            print(f"Не удалось прочитать данные из {file_name}")

# --- Визуализация ---
fig, axs = plt.subplots(1, 2, figsize=(12, 5))

# Время выполнения
axs[0].bar(labels, times, color='skyblue')
axs[0].set_title('Execution Time (seconds)')
axs[0].set_ylabel('Time (s)')
axs[0].set_xticklabels(labels, rotation=15, ha='right')

axs[0].set_ylim(0, max(times) + 5)

# Использование памяти
axs[1].bar(labels, memories, color='salmon')
axs[1].set_title('Memory Usage (MB)')
axs[1].set_ylabel('Memory (MB)')
axs[1].set_xticklabels(labels, rotation=15, ha='right')

axs[1].set_ylim(0, max(memories) + 5)

plt.tight_layout()
plt.show()

plt.savefig("comparison.png")