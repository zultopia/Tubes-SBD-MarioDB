t_S = 4e-5 # Disk Seek time
t_T = 2e-6 # Disk block transfer


# SSDs using the PCIe
# 3.0x4 interface have smaller tS , of 20 to 60 microseconds, and much higher transfer
# rates of around 2 gigabytes/second, corresponding to tT of 2 microseconds, allowing
# around 50,000 to 15,000 random 4-kilobyte block reads per second, depending on the
# model.3