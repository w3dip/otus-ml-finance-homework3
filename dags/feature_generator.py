import numpy as np

def create_lag_features(df, features, lag_periods):
    """
    Добавляет лаги для указанных признаков на указанное количество периодов назад.

    df: DataFrame с исходными данными
    features: список признаков, для которых необходимо добавить лаги
    lag_periods: сколько лагов назад необходимо создать
    Возвращает:
    - обновленный DataFrame с лагами
    - список новых колонок, которые можно использовать как признаки
    """
    df = df.copy()  # Работаем с копией DataFrame
    new_columns = []  # Список для хранения новых колонок

    # Для каждого признака создаем лаги
    for feature in features:
        for lag in range(1, lag_periods + 1):
            new_col_name = f'{feature}_lag_{lag}'
            df[new_col_name] = df[feature].shift(lag)
            new_columns.append(new_col_name)

    # Удаляем строки с NaN значениями, которые появились из-за сдвигов
    df = df.dropna()

    return df, new_columns

def create_rolling_features(df, features, window_sizes):
    """
    Добавляет скользящие характеристики для указанных признаков и окон.

    df: DataFrame с исходными данными
    features: список признаков, для которых необходимо добавить скользящие характеристики
    window_sizes: список размеров окон для расчета характеристик (например, [5, 14, 30])

    Возвращает:
    - обновленный DataFrame с новыми фичами
    - список новых колонок, которые можно использовать как признаки
    """
    df = df.copy()  # Работаем с копией DataFrame
    new_columns = []  # Список для хранения новых колонок

    # Для каждого признака и для каждого окна
    for feature in features:
        for window_size in window_sizes:
            # Скользящее среднее
            df[f'{feature}_mean_{window_size}'] = df[feature].rolling(window=window_size).mean()
            new_columns.append(f'{feature}_mean_{window_size}')

            # Скользящая медиана
            df[f'{feature}_median_{window_size}'] = df[feature].rolling(window=window_size).median()
            new_columns.append(f'{feature}_median_{window_size}')

            # Скользящий минимум
            df[f'{feature}_min_{window_size}'] = df[feature].rolling(window=window_size).min()
            new_columns.append(f'{feature}_min_{window_size}')

            # Скользящий максимум
            df[f'{feature}_max_{window_size}'] = df[feature].rolling(window=window_size).max()
            new_columns.append(f'{feature}_max_{window_size}')

            # Скользящее стандартное отклонение
            df[f'{feature}_std_{window_size}'] = df[feature].rolling(window=window_size).std()
            new_columns.append(f'{feature}_std_{window_size}')

            # Скользящий размах (макс - мин)
            df[f'{feature}_range_{window_size}'] = df[f'{feature}_max_{window_size}'] - df[f'{feature}_min_{window_size}']
            new_columns.append(f'{feature}_range_{window_size}')

            # Скользящее абсолютное отклонение от медианы (mad)
            df[f'{feature}_mad_{window_size}'] = df[feature].rolling(window=window_size).apply(lambda x: np.median(np.abs(x - np.median(x))), raw=True)
            new_columns.append(f'{feature}_mad_{window_size}')

    # Удаление строк с NaN значениями, которые появляются из-за сдвигов
    df = df.dropna()

    return df, new_columns

def create_trend_features(df, features, lag_periods):
    """
    Добавляет классические финансовые признаки: отношение к предыдущим периодам, логарифмические изменения и индикаторы трендов.

    df: DataFrame с исходными данными
    features: список признаков, для которых необходимо добавить индикаторы
    lag_periods: сколько периодов назад учитывать для расчетов

    Возвращает:
    - обновленный DataFrame с новыми фичами
    - список новых колонок, которые можно использовать как признаки
    """
    df = df.copy()  # Работаем с копией DataFrame
    new_columns = []  # Список для хранения новых колонок

    for feature in features:
        # Отношение текущего значения к предыдущему (лаг = 1)
        df[f'{feature}_ratio_1'] = df[feature] / df[feature].shift(1)
        new_columns.append(f'{feature}_ratio_1')

        # Логарифмическое изменение (логарифм отношения текущего значения к предыдущему)
        df[f'{feature}_log_diff_1'] = np.log(df[feature] / df[feature].shift(1))
        new_columns.append(f'{feature}_log_diff_1')

        # Momentum (разница между текущим значением и значением N периодов назад)
        df[f'{feature}_momentum_{lag_periods}'] = df[feature] - df[feature].shift(lag_periods)
        new_columns.append(f'{feature}_momentum_{lag_periods}')

        # Rate of Change (ROC): процентное изменение за N периодов
        df[f'{feature}_roc_{lag_periods}'] = (df[feature] - df[feature].shift(lag_periods)) / df[feature].shift(lag_periods) * 100
        new_columns.append(f'{feature}_roc_{lag_periods}')

        # Exponential Moving Average (EMA) с периодом N
        df[f'{feature}_ema_{lag_periods}'] = df[feature].ewm(span=lag_periods, adjust=False).mean()
        new_columns.append(f'{feature}_ema_{lag_periods}')

    # Удаление строк с NaN значениями, которые появились из-за сдвигов
    df = df.dropna()

    return df, new_columns

def create_macd(df, feature, short_window=12, long_window=26):
    """
    Добавляет индикатор MACD (разница между краткосрочным и долгосрочным EMA).

    df: DataFrame с исходными данными
    feature: признак, для которого необходимо рассчитать MACD
    short_window: окно для краткосрочного EMA (по умолчанию 12)
    long_window: окно для долгосрочного EMA (по умолчанию 26)

    Возвращает:
    - обновленный DataFrame с MACD
    - название новой колонки с MACD
    """
    df = df.copy()

    # Рассчитываем краткосрочное и долгосрочное EMA
    ema_short = df[feature].ewm(span=short_window, adjust=False).mean()
    ema_long = df[feature].ewm(span=long_window, adjust=False).mean()

    # Разница между краткосрочным и долгосрочным EMA (MACD)
    df[f'{feature}_macd'] = ema_short - ema_long

    return df, f'{feature}_macd'