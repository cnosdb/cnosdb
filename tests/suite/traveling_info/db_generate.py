
replace_dict = {
    'NOAA_water_database': 'traveling_info_database',
    'noaa': 'traveling_info',
    'average_temperature': 'avg_temperature',
    'h2o_temperature': 'max_temperature',
    'h2o_feet': 'information',
    'h2o_pH': 'recommended',
    'h2o_quality': 'air_quality',
    'coyote_creek': 'beijing',
    'santa_monica': 'shanghai',
    'water_level': 'human_traffic',
    'level description': 'description',
    'at or greater than 9 feet': 'very high traffic',
    'between 6 and 9 feet': 'high traffic',
    'between 3 and 6 feet': 'general traffic',
    'below 3 feet': 'less traffic',
    'pH': 'rec_level',
    'randtag': 'air_level',
    'index': 'air_score',
    'degrees': 'degree'
}

gap_char = {
    ' ',
    ',',
    '=',
    '"',
    ':',
    '(',
    ')',
    '{',
    '}',
    '.',
    '`'
}


def get_traveling_info_data():
    ori_path = "../noaa/NOAA_data.txt"
    gen_path = "./traveling_info_data.txt"

    rf = open(ori_path, "r")
    lines = rf.readlines()
    wf = open(gen_path, "w")
    # line = "h2o_feet,location=santa_monica water_level=2.995,level\ description=\"below 3 feet\" 1567244160"
    i = 1
    for line in lines:
        print(line)
        new_line = ""
        key_word = ""
        is_str = False
        ignore_char = False
        for c in line:
            if (c not in gap_char) or (ignore_char is True):
                if c == '\\':
                    ignore_char = True
                    continue
                if ignore_char is True:
                    ignore_char = False
                key_word += c
            else:
                if c == '"':
                    if is_str is False:
                        new_line += '"'
                        is_str = True
                        continue
                    else:
                        is_str = False
                if is_str is True:
                    key_word += c
                    continue
                if replace_dict.__contains__(key_word):
                    new_line += replace_dict[key_word]
                else:
                    new_line += key_word
                new_line += c
                key_word = ""
        new_line += key_word
        print(i, new_line)
        wf.write("{}".format(new_line))
        i += 1
    rf.close()
    wf.close()


def get_ti_data_case():
    ori_path = "../noaa/case_write.go"
    gen_path = "./case_write.go"

    rf = open(ori_path, "r")
    lines = rf.readlines()
    wf = open(gen_path, "w")

    i = 1

    for line in lines:
        print(line)
        new_line = ""
        key_word = ""
        is_str = False
        ignore_char = False
        for c in line:
            if (c not in gap_char) or (ignore_char is True):
                if c == '\\':
                    ignore_char = True
                    continue
                if ignore_char is True:
                    ignore_char = False
                key_word += c
            else:
                if c == '"':
                    if is_str is False:
                        new_line += '"'
                        is_str = True
                        continue
                    else:
                        is_str = False
                if is_str is True:
                    key_word += c
                    continue
                if replace_dict.__contains__(key_word):
                    new_line += replace_dict[key_word]
                else:
                    new_line += key_word
                new_line += c
                key_word = ""
        new_line += key_word
        print(i, new_line)
        wf.write("{}".format(new_line))
        i += 1
        # if i > 200:
        #     break
    rf.close()
    wf.close()


if __name__ == '__main__':
    # get_traveling_info_data()
    get_ti_data_case()
