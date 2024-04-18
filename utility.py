

def day_mapping():
    days_of_the_week = ['monday','tuesday','wednesday','thursday','friday','saturday','sunday']
    day_mapping ={}
    for i,j in zip(range(7),days_of_the_week):
        day_mapping[i] = j
    return day_mapping