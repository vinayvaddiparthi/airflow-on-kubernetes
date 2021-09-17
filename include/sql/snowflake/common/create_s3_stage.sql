create stage if not exists {{ params.stage_name }}
    file_format =(type={{ params.file_format }});