from logging import log
import sys
import os

LOG_NUM = 16

COMPRESS_LSN = False

if len(sys.argv) < 2:
    print('Usage: %s logFile' % sys.argv[0])
    sys.exit(0)

LOCK_BIT = 1<<63

logfile = sys.argv[1]

log_alg = logfile.split('/')[-1][0]
LOG_NUM = int(logfile.split('/')[-1].split('_')[2])

LV_MAX = os.path.getsize(logfile)

f = open(logfile, "rb")
counter = 0
offset = 0
LVFence = [0] * LOG_NUM
while True:
    header = f.read(8)
    offset += 8
    if len(header) == 0:
        break
    assert(len(header) == 8)
    checksum = int.from_bytes(header[:4], byteorder='little', signed=False)
    print(hex(checksum))
    assert(checksum == 0xbeef or checksum & 0xff == 0x7f)
    size = int.from_bytes(header[4:], byteorder='little', signed=False)
    size_aligned = size
    print(size)
    if size % 64 != 0:
        size_aligned = size + 64 - size % 64
    content = f.read(size_aligned-8)
    
    if size_aligned == 64:
        print((header + content).hex())
    offset += size_aligned - 8

    if log_alg == 'T':

        lv_i = [k for k in LVFence] # copy
        cost = 0
        if COMPRESS_LSN:
            if checksum & 0xff == 0x7f:
                # PLV
                for i in range(LOG_NUM):
                    LVFence[i] = int.from_bytes(content[i*8:i*8+8], byteorder="little", signed=False)
                print("Update LVFence to", ','.join([ str(li) for li in LVFence]), "costing", LOG_NUM * 8)
                continue
            else:
                num = int(content[size -8 -1])
                for i in range(num):
                    lv_content = int.from_bytes(content[size-1-8 -8*i-8:size-1-8 -8*i], byteorder="little", signed=False)
                    lv_i[lv_content & ((1<<5)-1)] = lv_content >> 5
                cost = 1 + num * 8
        else:
            for i in range(LOG_NUM):
                lv_content = int.from_bytes(content[size-8 -i*8-8:size-8 -i*8], byteorder="little", signed=False)
                lv_i[LOG_NUM - 1 - i] = lv_content
                # assert(lv_content < LV_MAX)
            cost = LOG_NUM * 8
        print("log[%d] of size %d at offset %d: [%s]" % (counter, size_aligned, offset, ','.join([ str(li) for li in lv_i])),
        "costing", cost
        )
        assert(len(content) > LOG_NUM * 8)
    
    elif log_alg == 'V':
        gsn = int.from_bytes(content[-8:], byteorder="little", signed=False)
        primary_record = False
        if (gsn & LOCK_BIT) > 0:
            primary_record = True
            gsn -= LOCK_BIT
        print("log[%d] of size %d at offset %d: GSN %d, primari_record: %d" % (counter, size_aligned, offset, gsn, primary_record))
    elif log_alg == 'S':
        print("log[%d] of size %d at offset %d" % (counter, size_aligned, offset))
    else:
        assert(False) # not implemented
    counter += 1
    if counter % 10000 == 0:
        print(counter)
        #break

f.close()
print("In total %d txns." % counter)
