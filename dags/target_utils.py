# Функция для добавления целевой переменной (таргета)
def add_target(df):
    df['close_next_hour'] = df['close'].shift(-1)
    df['target'] = (df['close_next_hour'] > df['close']).astype(int)
    df = df.dropna(subset=['close_next_hour'])
    return df

# Функция для фильтрации строк, где таргет не определён (например, NaN в close_next_hour)
def filter_invalid_targets(df):
    # Удаляем строки, где close_next_hour или target равен NaN
    return df.dropna(subset=['close_next_hour', 'target'])