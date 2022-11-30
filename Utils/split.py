chunk_size = 400000


def write_chunk(part, lines):
    with open('salida' + '.csv', 'w') as f_out:
        f_out.write(header)
        f_out.writelines(lines)


terminar = False


with open("itineraries_3GB.csv", "r") as f:
    count = 0
    header = f.readline()
    lines = []
    for line in f:
        count += 1
        lines.append(line)
        if count % chunk_size == 0:
            write_chunk(count // chunk_size, lines)
            lines = []
            break
    # write remainder
    if len(lines) > 0:
        write_chunk((count // chunk_size) + 1, lines)
