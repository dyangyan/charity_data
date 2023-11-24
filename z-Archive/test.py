charity_name = ["A"]
reg_num = ["123"]
urls = ["abc", "def"]

max_length = max(len(charity_name), len(reg_num), len(urls))

result_dict = {
    "charity_name": [charity_name[i % len(charity_name)] for i in range(max_length)],
    "reg_num": [reg_num[i % len(reg_num)] for i in range(max_length)],
    "urls": urls,
}

print(result_dict)
