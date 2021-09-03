import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

df_a = pd.read_csv('Приложение А', sep='\t')
df_b = pd.read_csv('Приложение B', sep='\t')
df_c = pd.read_csv('Приложение C', sep='\t')

sum_time_per_task = df_a.groupby('Задача')\
                          .agg({'Часы': 'sum'})\
                          .reset_index()\
                          .sort_values('Часы')


def delimiter():
    print('-------------------------------------------------------------------------')


print('Общие трудозатраты на проект: ', sum_time_per_task['Часы'].sum(), ' ч.', sep='')
delimiter()
print('Среднее время, затраченное на решение задач: ', round(sum_time_per_task['Часы'].mean()), ' ч.', sep='')
delimiter()
print('Медианное время, затраченное на решение задач: ', round(sum_time_per_task['Часы'].median()), ' ч.', sep='')
delimiter()

average_time_for_each_worker = df_a.groupby('Исполнитель').agg({'Часы': 'mean'}).reset_index()
average_time_for_each_worker['Часы'] = average_time_for_each_worker['Часы'].round(1)

print('Среднее время, затраченное на решение задач каждым из исполнителей:', average_time_for_each_worker, sep='\n')
delimiter()

sum_time_for_each_worker = df_a.groupby('Исполнитель').agg({'Часы': 'sum'}).reset_index()
sum_time_for_each_worker = sum_time_for_each_worker.merge(df_c)
sum_time_for_each_worker['Суммарный заработок'] = sum_time_for_each_worker['Часы'] * sum_time_for_each_worker['Ставка']

expenses = sum_time_for_each_worker['Суммарный заработок'].sum()
income = 24000
profit = income - expenses
profitability = round((profit * 100) / income, 1)
print(f'Рентабельность проекта - {profitability}%')
delimiter()

average_time_for_each_worker_per_day = df_a.groupby(['Дата', 'Исполнитель'])\
                        .agg({'Часы': 'sum'})\
                        .groupby('Исполнитель')\
                        .agg({'Часы': 'mean'})\
                        .reset_index()
average_time_for_each_worker_per_day['Часы'] = round(average_time_for_each_worker_per_day['Часы'], 1)
print(
    'Cреднее количество часов, отрабатываемое каждым сотрудником за день:',
    average_time_for_each_worker_per_day,
    sep='\n'
    )
delimiter()

df_a['Дата'] = pd.to_datetime(df_a['Дата'])

working_days = pd.DataFrame(df_a[df_a['Дата'].dt.dayofweek < 6])\
                 .drop(['Задача', 'Часы'], axis=1)\
                 .drop_duplicates()

workers = pd.DataFrame(working_days['Исполнитель'].unique(), columns={'Исполнитель'})
dates = pd.DataFrame(working_days['Дата'].unique(), columns={'Дата'})
dates_workers_cross = dates.merge(workers, how='cross')

workers_absence_days = pd.merge(
    dates_workers_cross,
    working_days,
    on=['Дата', 'Исполнитель'],
    how="outer",
    indicator=True
)

workers_absence_days = workers_absence_days.query('_merge=="left_only"')\
                                           .reset_index()\
                                           .drop(['_merge', 'index'], axis=1)
print('Дни отсутствия для каждого сотрудника:', workers_absence_days, sep='\n')
delimiter()

timing_failure = df_a.groupby(['Исполнитель', 'Задача'], as_index=False)\
                     .agg({'Часы': 'sum'})\
                     .merge(df_b)

timing_failure['Вылет'] = (timing_failure['Часы'] - timing_failure['Оценка']) / timing_failure['Оценка'] * 100

timing_failure = round(timing_failure.groupby('Исполнитель', as_index=False).agg({'Вылет': 'mean'}), 1)
print('Cредний «вылет» специалиста из оценки, %', timing_failure, sep='\n')
delimiter()

estimated_actual = sum_time_per_task.merge(df_b)
estimated_actual['Задача1'] = estimated_actual['Задача'].map(lambda x: int(x[4:]))
estimated_actual = estimated_actual.sort_values(by='Задача1')

plt.rcParams['figure.figsize'] = (16, 6)

x = np.arange(len(estimated_actual['Задача']))
width = 0.35

fig, ax = plt.subplots()

rects1 = ax.bar(
    x - width/2,
    estimated_actual['Часы'],
    width,
    label='Фактические трудозатраты',
    color='lightcoral',
    edgecolor='black',
    linewidth=0.5
)

rects2 = ax.bar(
    x + width/2,
    estimated_actual['Оценка'],
    width,
    label='Оценка',
    color='lightblue',
    edgecolor='black',
    linewidth=0.5
)

ax.set_ylabel('Время, ч')
ax.set_xlabel('Задача')
ax.set_title('Сводный график по каждой из задач проекта')
ax.set_xticks(x)
ax.set_xticklabels(estimated_actual['Задача1'])
ax.legend()

ax.bar_label(rects1, padding=3)
ax.bar_label(rects2, padding=3)

fig.tight_layout()

plt.savefig('plot.png', dpi=200)
print('Сводный график "plot.png" сохранен.')