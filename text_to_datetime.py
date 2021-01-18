def text_to_datetime(text,convert_to_local='Y'):
    if convert_to_local=='Y':
        local_timezone='Asia/Ho_Chi_Minh'
        result=parser.parse(text).astimezone(pytz.timezone(local_timezone))
    else:
        result=parser.parse(text)
    return(result)
