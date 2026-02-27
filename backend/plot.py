{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "41df0e40-97db-4e44-992b-a2c6c6cd5077",
   "metadata": {},
   "outputs": [],
   "source": [
    "import polars as pl\n",
    "import pandas as pd\n",
    "import numpy as np\n",
    "from datetime import datetime\n",
    "import psycopg2 as psql\n",
    "import deltalake\n",
    "import duckdb\n",
    "import pyfolio\n",
    "import empyrical as strata\n",
    "import os, zlib, time\n",
    "import asyncio\n",
    "\n",
    "from pyecharts.faker import Faker\n",
    "import pyecharts.options as opts\n",
    "import pyecharts.charts as echarts\n",
    "from pyecharts.globals import ThemeType, CurrentConfig, NotebookType \n",
    "\n",
    "from IPython.display import HTML, display, clear_output"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "75a14cf5-e9c9-433c-9afb-5eaf1b0537c2",
   "metadata": {},
   "outputs": [],
   "source": [
    "#inputs_address_view = \"/ssd-btc/views/inputs_address\"\n",
    "#outputs_address_view  = \"s3a://fregat1917/btc/views/outputs_address\"\n",
    "inputs_address_view = \"/data/btc/views/inputs_address\"\n",
    "outputs_address_view  = \"/data/btc/views/outputs_address\""
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "e5f4e631-9ec4-44cb-8f8f-43575af21ce7",
   "metadata": {},
   "outputs": [],
   "source": [
    "storage_options={\n",
    "        \"AWS_ACCESS_KEY_ID\": os.getenv('AWS_ACCESS_KEY_ID'),\n",
    "        \"AWS_SECRET_ACCESS_KEY\": os.getenv('AWS_SECRET_ACCESS_KEY'),\n",
    "        \"AWS_ENDPOINT_URL\": \"https://hel1.your-objectstorage.com\",\n",
    "        \"AWS_S3_ALLOW_UNSAFE_RENAME\": \"true\",\n",
    "        \"AWS_ALLOW_HTTP\": \"true\"\n",
    "}"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "008b21be-91d5-427d-83d1-e40d9b1be0b5",
   "metadata": {},
   "outputs": [],
   "source": [
    "#connect to DB\n",
    "db = psql.connect(database=\"btc\", user=\"postgres\", password=\"dbadmin1975\", host=\"postgres17\", port=\"5432\")\n",
    "db.autocommit = False\n",
    "cursor = db.cursor()\n",
    "sql_query = \"SELECT date_, close_ FROM quotes WHERE date_ >= '1900-01-01' AND date_ <= '2025-12-31' ORDER BY date_\"\n",
    "btc_quotes = pd.read_sql(sql_query, db)\n",
    "btc_quotes = pl.from_pandas(btc_quotes)\n",
    "btc_quotes = btc_quotes.with_columns(pl.col('date_').cast(pl.Date))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "a0e3772b-911d-4b8b-9dcf-bc53a868c4c7",
   "metadata": {},
   "outputs": [],
   "source": [
    "def get_df_in(address, table_path): # когда надо получить все транзакции\n",
    "    partition = zlib.crc32(address.encode('utf-8')) % 1000\n",
    "    df = pl.scan_delta(table_path, use_pyarrow=True,\n",
    "      pyarrow_options={\n",
    "          \"partitions\": [(\"partition_\", \"=\", str(partition))]\n",
    "        }\n",
    "      )\n",
    "    df = df.filter(pl.col('address') == address)\n",
    "    df = df.select('t_time','address','t_value','exit_usdvalue')\n",
    "    df = df.rename({\"exit_usdvalue\": \"t_usdvalue\"})\n",
    "    return df.collect()\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "d988ed01-3e93-4d1c-b947-43522a1a0a94",
   "metadata": {},
   "outputs": [],
   "source": [
    "def get_df_out(address, table_path): # когда надо получить все транзакции\n",
    "    storage_options = {}\n",
    "    partition = zlib.crc32(address.encode('utf-8')) % 1000\n",
    "    df = pl.scan_delta(table_path, use_pyarrow=True, storage_options=storage_options,\n",
    "      pyarrow_options={\n",
    "          \"partitions\": [(\"partition_\", \"=\", str(partition))]\n",
    "        }\n",
    "      )\n",
    "    df = df.filter(pl.col('address') == address)\n",
    "    df = df.select('t_time','address','t_value','t_usdvalue')\n",
    "    return df.collect()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "5a76b059-64b4-4064-ab43-b03f205131a4",
   "metadata": {},
   "outputs": [],
   "source": [
    "#%autoawait asyncio\n",
    "start_time = time.time()\n",
    "address = '3HjSc5fZFRepr5KB5sdtkLqLdxoCNysad8'\n",
    "df_in = get_df_in(address, inputs_address_view)\n",
    "df_out = get_df_out(address, outputs_address_view)\n",
    "#df_in, df_out = await asyncio.gather(get_df_in(address, inputs_address_view), get_df_out(address, outputs_address_view))\n",
    "end_time = time.time()\n",
    "elapsed_time = end_time - start_time\n",
    "print(f\"Time taken: {elapsed_time} seconds\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "f895fd58-b85e-43e1-8db5-851f1a64f36c",
   "metadata": {},
   "outputs": [],
   "source": [
    "def plot(df):\n",
    "    # Plot Settings\n",
    "    CurrentConfig.NOTEBOOK_TYPE = NotebookType.JUPYTER_LAB\n",
    "    datazoom_opts=[opts.DataZoomOpts(type_='slider', range_start=0, range_end=100),\n",
    "         opts.DataZoomOpts(type_='inside')\n",
    "     ]\n",
    "    yaxis_opts = opts.AxisOpts(name = 'Equity')\n",
    "    xaxis_opts = opts.AxisOpts(name = 'Date', splitline_opts=opts.SplitLineOpts(is_show=True))\n",
    "    toolbox_opts=opts.ToolboxOpts(is_show=True, feature={\"dataZoom\": {\"yAxisIndex\": 'none'},\"saveAsImage\": {'backgroundColor': '#fff'}})\n",
    "    tooltip_opts=opts.TooltipOpts(trigger='axis', axis_pointer_type='shadow')\n",
    "    x_data = df['date_'].tolist()\n",
    "    y1_data = df['value'].tolist()\n",
    "    y2_data = df['close_'].round(2).tolist()\n",
    "    #volume_data = df['total_volume']/1000000\n",
    "    #.round(3).tolist()\n",
    "    plot = (\n",
    "        echarts.Line(init_opts=opts.InitOpts(theme=ThemeType.WHITE))\n",
    "        .add_xaxis(x_data)\n",
    "        .add_yaxis(\"USD Profit/Loss\", y1_data, yaxis_index=0, is_symbol_show=False, linestyle_opts=opts.LineStyleOpts(color=\"blue\"),\n",
    "        itemstyle_opts=opts.ItemStyleOpts(color=\"blue\", border_color=\"#FFF\", border_width=1))\n",
    "        .add_yaxis(\"Bitcoin Price\", y2_data, yaxis_index=1, is_symbol_show=False,\n",
    "            linestyle_opts=opts.LineStyleOpts(color=\"red\"),\n",
    "            label_opts=opts.LabelOpts(is_show=True, position=\"top\", color=\"red\"),\n",
    "            itemstyle_opts=opts.ItemStyleOpts(color=\"red\", border_color=\"#fff\", border_width=2))\n",
    "        .extend_axis( yaxis=opts.AxisOpts(axislabel_opts=opts.LabelOpts(formatter=\"\") )  )\n",
    "        .set_global_opts(title_opts=opts.TitleOpts(title=\"\"),\n",
    "                         datazoom_opts=datazoom_opts, xaxis_opts = xaxis_opts, yaxis_opts = yaxis_opts,\n",
    "                         toolbox_opts = toolbox_opts, tooltip_opts = tooltip_opts)\n",
    "\n",
    "        )\n",
    "    #plot.render_notebook()\n",
    "    display(HTML(plot.render(\"echarts.html\")))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "0fb543a2-bf4d-4137-b8b6-b075f2af22f8",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Вычисления для выбранного адреса\n",
    "def get_address_transactions(btc_addr, trans_in, trans_out, btc_prices):\n",
    "    trans_out = trans_out.filter(pl.col('address') == btc_addr)\n",
    "    trans_in = trans_in.filter(pl.col('address') == btc_addr)\n",
    "    trans_out = trans_out.sort('t_time', descending = False)\n",
    "    # конвертируем время в дату\n",
    "    trans_in = trans_in.with_columns(pl.col('t_time').cast(pl.Date).alias('date_'))\n",
    "    trans_out = trans_out.with_columns(pl.col('t_time').cast(pl.Date).alias('date_'))\n",
    "    first_output = trans_out.select(pl.col('date_')).row(0)[0]\n",
    "    btc_prices = btc_prices.filter(pl.col('date_') >= first_output)\n",
    "    # UNION IN and OUT trans (in_transactions with minus  \n",
    "    # мы берем со знаком минус t_value из inputs и t_usdvalue из outputs, так как отдаем битки и получаем баксы\n",
    "    df1 = trans_in.with_columns(pl.col(\"t_value\") * (-1))\n",
    "    df2 = trans_out.with_columns(pl.col(\"t_usdvalue\") * (-1))\n",
    "    trans_all = pl.concat([df1, df2]) \n",
    "    # Сортировка по возрастанию дат\n",
    "    trans_all = trans_all.sort('t_time', descending = False)\n",
    "    # группировка по датам для этого кошелька\n",
    "    #trans_all = trans_all.with_columns(pl.col('t_time').cast(pl.Date).alias('date_'))\n",
    "    trans_all = trans_all.group_by(\"date_\").agg(pl.col(\"t_value\").sum(), pl.col(\"t_usdvalue\").sum()) \n",
    "    # Объединяем с ценами на биток\n",
    "    df = btc_prices.join(trans_all , on='date_', how='left')\n",
    "    #df = df.with_columns(pl.col('t_value').fill_null(0))\n",
    "    df = df.fill_null(0)\n",
    "    # добавляем нарастающий итог t_value & t_usdvalue на каждую дату, поле _cumsum\n",
    "    df = df.with_columns(pl.col(\"t_value\").cum_sum().name.suffix(\"_cumsum\"),)  \n",
    "    df = df.with_columns(pl.col(\"t_usdvalue\").cum_sum().name.suffix(\"_cumsum\"),) \n",
    "    # Защита от того что мы продаем битки которых у нас нет\n",
    "    # Если мы ушли в минус по биткам то обнуляем эту транзакцию и потом считаем нарастающий итог по новой\n",
    "    df = df.with_columns(pl.when(pl.col(\"t_value_cumsum\") >= 0).then(pl.col(\"t_usdvalue\")).otherwise(0).alias('t_usdvalue'))\n",
    "    df = df.with_columns(pl.when(pl.col(\"t_value_cumsum\") >= 0).then(pl.col(\"t_value\")).otherwise(0).alias('t_value'))\n",
    "    df = df.with_columns(pl.col(\"t_value\").cum_sum().name.suffix(\"_cumsum\"),)  \n",
    "    df = df.with_columns(pl.col(\"t_usdvalue\").cum_sum().name.suffix(\"_cumsum\"),) \n",
    "    # считаем маржу, это уже будет нарастающий итог(!!!)\n",
    "    df = df.with_columns(  (pl.col('t_usdvalue_cumsum') + pl.col('t_value_cumsum')*pl.col('close_')/100000000).alias('delta_usd')  )    \n",
    "    # считаем ежедневный return в процентах (тело считаем как t_value_cumsum*btc_price) либо как max(t_value_cumsum)\n",
    "    # initial_value как максимальный приход денег в рынок, когда покупаем btc то t_usdvalue со знаком минус\n",
    "    initial_value = abs(df.select(pl.col(\"t_usdvalue_cumsum\")).min().item(0,0))\n",
    "    max_btc_value = df.select(pl.col(\"t_value_cumsum\").abs()).max().item(0,0)\n",
    "    df = df.with_columns( (pl.col(\"delta_usd\") + initial_value).pct_change().alias(\"returns\"))\n",
    "    df = df.with_columns( pl.col('returns').log1p().alias(\"log_returns\")) # переводим в logNormal returns (natural logarithm)\n",
    "    #df = df.with_columns(pl.col(\"returns\").cum_sum().name.suffix(\"_cumsum\"),) \n",
    "    #df = df.with_columns(pl.col(\"returns_cumsum\").exp())\n",
    "    df = df.with_columns( (pl.col(\"t_value_cumsum\") / max_btc_value).alias(\"exposure\"))\n",
    "    exposure = df['exposure'].mean()\n",
    "    profit_pct = df[\"log_returns\"].sum()\n",
    "    profit_pct = np.exp(profit_pct) - 1\n",
    "    df = df.fill_nan(0)\n",
    "    # вычисляем количество дней в рынке с битком на руках\n",
    "    count_days_in_market = len(df.filter(pl.col('t_value_cumsum') > 100000000))\n",
    "    df = df.with_columns(pl.col('delta_usd').alias('value')).to_pandas()\n",
    "    #df = df.with_columns(pl.col('returns_cumsum').alias('value')).to_pandas()\n",
    "    #df = df.fillna(0)\n",
    "    sharp_ratio = strata.sharpe_ratio(df.returns, risk_free=0, period='daily')\n",
    "    drawdown = strata.max_drawdown(df.returns)\n",
    "    benchmark_sharpe, benchmark_drawdown, benchmark_profit = benchmark(btc_prices) \n",
    "    #print(benchmark_sharp, benchmark_drawdown)\n",
    "    # добавляем нарастающий итог стоимости портфеля на каждую дату, поле _cumsum\n",
    "    #trans_all = trans_all.with_columns(pl.col(\"t_usdvalue\").cum_sum().name.suffix(\"_cumsum\"),)\n",
    "    #trans_all.sort('date_', descending = False)\n",
    "    return df, sharp_ratio, drawdown, exposure, benchmark_sharpe, benchmark_drawdown, count_days_in_market, profit_pct, benchmark_profit    "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "971be5e1-d823-4e97-b6e2-948294e32580",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Вычисления для выбранного адреса\n",
    "def get_address_balance(btc_addr, trans_in, trans_out, btc_prices):\n",
    "    trans_out = trans_out.filter(pl.col('address') == btc_addr)\n",
    "    trans_in = trans_in.filter(pl.col('address') == btc_addr)\n",
    "    trans_out = trans_out.sort('t_time', descending = False)\n",
    "    # конвертируем время в дату\n",
    "    trans_in = trans_in.with_columns(pl.col('t_time').cast(pl.Date).alias('date_'))\n",
    "    trans_out = trans_out.with_columns(pl.col('t_time').cast(pl.Date).alias('date_'))\n",
    "    first_output = trans_out.select(pl.col('date_')).row(0)[0]\n",
    "    btc_prices = btc_prices.filter(pl.col('date_') >= first_output)\n",
    "    # UNION IN and OUT trans (in_transactions with minus  \n",
    "    # мы берем со знаком минус t_value из inputs и t_usdvalue из outputs, так как отдаем битки и получаем баксы\n",
    "    df1 = trans_in.with_columns(pl.col(\"t_value\") * (-1))\n",
    "    df2 = trans_out.with_columns(pl.col(\"t_usdvalue\") * (-1))\n",
    "    trans_all = pl.concat([df1, df2]) \n",
    "    # Сортировка по возрастанию дат\n",
    "    trans_all = trans_all.sort('t_time', descending = False)\n",
    "    # группировка по датам для этого кошелька\n",
    "    #trans_all = trans_all.with_columns(pl.col('t_time').cast(pl.Date).alias('date_'))\n",
    "    trans_all = trans_all.group_by(\"date_\").agg(pl.col(\"t_value\").sum(), pl.col(\"t_usdvalue\").sum()) \n",
    "    # Объединяем с ценами на биток\n",
    "    df = btc_prices.join(trans_all , on='date_', how='left')\n",
    "    df = df.fill_null(0)\n",
    "    # добавляем нарастающий итог t_value & t_usdvalue на каждую дату, поле _cumsum\n",
    "    df = df.with_columns(pl.col(\"t_value\").cum_sum().name.suffix(\"_cumsum\"),)  \n",
    "    df = df.with_columns(pl.col(\"t_usdvalue\").cum_sum().name.suffix(\"_cumsum\"),) \n",
    "    # Защита от того что мы продаем битки которых у нас нет\n",
    "    # Если мы ушли в минус по биткам то обнуляем эту транзакцию и потом считаем нарастающий итог по новой\n",
    "    df = df.with_columns(pl.when(pl.col(\"t_value_cumsum\") >= 0).then(pl.col(\"t_usdvalue\")).otherwise(0).alias('t_usdvalue'))\n",
    "    df = df.with_columns(pl.when(pl.col(\"t_value_cumsum\") >= 0).then(pl.col(\"t_value\")).otherwise(0).alias('t_value'))\n",
    "    df = df.with_columns(pl.col(\"t_value\").cum_sum().name.suffix(\"_cumsum\"),)  \n",
    "    df = df.with_columns(pl.col(\"t_usdvalue\").cum_sum().name.suffix(\"_cumsum\"),) \n",
    "    df = df.with_columns(pl.col('t_value_cumsum').alias('value')).to_pandas()\n",
    "    return df  "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "f8183e07-a590-4dbc-89f0-9f76f6b90997",
   "metadata": {},
   "outputs": [],
   "source": [
    "def benchmark(df):\n",
    "    df = df.with_columns( pl.col(\"close_\").pct_change().alias(\"returns\")).to_pandas()\n",
    "    sharpe = strata.sharpe_ratio(df.returns, risk_free=0, period='daily')\n",
    "    drawdown = strata.max_drawdown(df.returns)\n",
    "    profit_pct = (df['close_'].iloc[-1] / df['close_'].iloc[0] - 1) \n",
    "    return sharpe, drawdown, profit_pct"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "1bca99dd-d8bb-4181-9e44-6f6d82a3619a",
   "metadata": {},
   "outputs": [],
   "source": [
    "%%time\n",
    "# !!! PLOT ADDRESS EQUITY\n",
    "btc_address = '1DD79tMgTa9kf923Bgy6eSyVdmfpMrpHfp'\n",
    "df_in = get_df_in(btc_address, inputs_address_view)\n",
    "df_out = get_df_out(btc_address, outputs_address_view)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "3fe095c1-bf8f-4d7d-b6e2-05044729e28f",
   "metadata": {},
   "outputs": [],
   "source": [
    "df, sharp_ratio, drawdown, exposure, benchmark_sharpe, benchmark_drawdown, count_days,profit_pct, benchmark_profit = get_address_transactions(btc_address, df_in, df_out, btc_quotes)\n",
    "print(\"Profit - \", round(profit_pct*100, 2), \"Profit Benchmark - \", round(benchmark_profit*100, 2))\n",
    "print(\"Sharp Ratio - \", round(sharp_ratio, 2), \"Sharp Ratio Benchmark - \", round(benchmark_sharpe, 2))\n",
    "print(\"Drawdown \", round(drawdown*100, 2), \"Drawdown Benchmark \", round(benchmark_drawdown*100,2))\n",
    "print(sharp_ratio, drawdown, exposure, profit_pct, 'Benchmark: ',benchmark_sharpe, benchmark_drawdown)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "778eb3b6-7885-464f-8371-f052083f24b8",
   "metadata": {},
   "outputs": [],
   "source": [
    "df = get_address_balance(btc_address, df_in, df_out, btc_quotes)\n",
    "plot(df)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "30b4723d-e892-456a-a4d0-8d7229e46e6b",
   "metadata": {},
   "outputs": [],
   "source": [
    "def plot_address(btc_address):\n",
    "    df_in = get_df_in(btc_address, inputs_address_view)\n",
    "    df_out = get_df_out(btc_address, outputs_address_view)\n",
    "    df, sharp_ratio, drawdown, exposure, benchmark_sharpe, benchmark_drawdown, count_days,profit_pct, benchmark_profit = get_address_transactions(btc_address, df_in, df_out, btc_quotes)\n",
    "    print(\"Profit - \", round(profit_pct*100, 2), \"Profit Benchmark - \", round(benchmark_profit*100, 2))\n",
    "    print(\"Sharp Ratio - \", round(sharp_ratio, 2), \"Sharp Ratio Benchmark - \", round(benchmark_sharpe, 2))\n",
    "    print(\"Drawdown \", round(drawdown*100, 2), \"Drawdown Benchmark \", round(benchmark_drawdown*100,2))\n",
    "    print(sharp_ratio, drawdown, exposure, profit_pct, 'Benchmark: ',benchmark_sharpe, benchmark_drawdown)\n",
    "    df = get_address_balance(btc_address, df_in, df_out, btc_quotes)\n",
    "    plot(df)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "680fc9f8-a6fd-4499-91db-0b542159841a",
   "metadata": {},
   "outputs": [],
   "source": [
    "plot_address('bc1qqfrqd0sprqfj0cfxng0yamhjpr80c84x458gnf')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "66b83fae-4bc4-46e0-a4ee-6ae42848cd7f",
   "metadata": {},
   "outputs": [],
   "source": [
    "i = 0\n",
    "pnl_stats = \"/data/btc/pnl_stats\"\n",
    "day_ = '2023-10-01'\n",
    "df_pnl = pl.scan_delta(pnl_stats).filter(pl.col('date_') == day_)\n",
    "df_pnl = df_pnl.with_columns([pl.col(\"btcvalue\").cast(pl.Int64), pl.col(\"max_btc\").cast(pl.Int64),])\n",
    "df1 = df_pnl.filter((pl.col('profit2btc') > 69000) & (pl.col('max_btc') > 10000000) \n",
    "              & (pl.col('count_out') >= 0)  & (pl.col('btcvalue') > -100000) \n",
    "              & (pl.col('first_in').cast(pl.Date) > datetime(2006, 1, 1)))\n",
    "df1 = df_pnl.filter((pl.col('profit2btc') > 40000) & (pl.col('max_btc') > 100000000) & (pl.col('btcvalue') > 0.7*pl.col('max_btc')) \n",
    "              & (pl.col('count_out') >= 1)\n",
    "              & (pl.col('first_in').cast(pl.Date) > datetime(20, 1, 1)))\n",
    "addresses = df1.sort('profit2btc', descending = True).collect()\n",
    "addresses = addresses['address'].to_list()\n",
    "for address in addresses:\n",
    "  i = i + 1\n",
    "  if i > 0:\n",
    "    print (i)\n",
    "    print(address)\n",
    "    if i == 300:\n",
    "        break    # break here\n",
    "    plot_address(address)\n",
    "    clear_output(wait=True)\n",
    "    input(\"Press Enter to continue...\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "75d3f884-a23c-4576-acfc-726a3716a201",
   "metadata": {},
   "outputs": [],
   "source": [
    "addresses1 = pl.read_csv('addresses.csv')  \n",
    "addresses2 = pl.read_csv('addresses2.csv')\n",
    "df = addresses1.join(addresses2, on='address', how='anti')\n",
    "addresses = df['address'].to_list()\n",
    "len(addresses)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "3fa3d5f8-dc9e-4a02-805c-1da1a7f54268",
   "metadata": {},
   "outputs": [],
   "source": [
    "addresses = ['bc1qtelc8y95up3my685c7enftk83r5r97phy9xr3n',\n",
    " '33mjsSSyPapQcEGLV5Uz8UTUiChaRpNMD2',\n",
    " '399vTBYF6S3jabrZT89zqCVGtuSHLdVvTE',\n",
    " 'bc1qnzrjk6f8yueyn35mkxlqxz0kr6u3ssq73awd0e',\n",
    " 'bc1qvlfsswe4egggh5sfdmej6qw3njsuxjtkutw2fm',\n",
    " 'bc1qaypmeh90tfyzzxl0n5u2m2d3xdk8xgam875zdl',\n",
    " 'bc1qfcdz05svwt3ra9c7nxvwcsj3vc5j6l60fzzzkr',\n",
    " '1zNNPaAH3zcc4BpwVKmWGCR9oBwHZae5t',\n",
    " 'bc1q6lwl9qx8ec3m7gspqmfsx3xeftzgzjn2r5j6j6',\n",
    " 'bc1qgxfkd6en9nx9kn60syqwlvu7qs4s98qg335qyp',\n",
    " 'bc1q0gf8trq26jpyu99u7kk8cc3n0uvzfrpxjc6mpj',\n",
    " 'bc1qgd9ajcf5djgqtwynnx8rc9g0rvfpjskunfepee',\n",
    " 'bc1q8m7q7ruhdh2dutthv22t2r9txxp4d26mqr5r8k',\n",
    " 'bc1quww9gfcl5lxwy6p4mj6cs2sqmwv9rq4kguusks',\n",
    " 'bc1q4nk5rsf2t4hw9gyys4xju6k44g6aynp2ll076c',\n",
    " '1AHTXmJdo6rAwBZtaNqAAjVrKejs7sEj4o',\n",
    " '1J4Hu8sh8MNpqNsFVaNFkizE4Cem1oxTU']\n",
    "i = 0\n",
    "for address in addresses:\n",
    "  i = i + 1\n",
    "  if i > 0:\n",
    "    print (i)\n",
    "    print(address)\n",
    "    if i == 300:\n",
    "        break    # break here\n",
    "    plot_address(address)\n",
    "    clear_output(wait=True)\n",
    "    input(\"Press Enter to continue...\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "796e7f6a-92e2-425c-b0a7-599f327dacf4",
   "metadata": {},
   "outputs": [],
   "source": [
    "duckdb.sql(\"\"\"SET memory_limit = '40GB';\"\"\")\n",
    "duckdb.sql(\"\"\"SET temp_directory = '/data/btc/tempdb/duckdb_swap';\"\"\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "c9b83bc9-da26-487f-b8d5-10711f52c8c9",
   "metadata": {},
   "outputs": [],
   "source": [
    "def get_data(partition, delta_table):\n",
    "    dataset = delta_table.to_pyarrow_dataset()  # !!! We SHOULD USE ARROW DATASET\n",
    "    df = duckdb.arrow(dataset)\n",
    "    query_from_delta = f\"SELECT * FROM df WHERE partition_ == {partition} order by t_time desc LIMIT 100;\"\n",
    "    return duckdb.query(query_from_delta).pl()\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "9f1d3e50-d566-4257-92b6-1841662d57db",
   "metadata": {},
   "outputs": [],
   "source": [
    "%%time\n",
    "delta_table = deltalake.DeltaTable(inputs_address_view, log_buffer_size = 48)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "5ebdbd62-bd19-4db4-bd74-f8ceaf3afd9b",
   "metadata": {},
   "outputs": [],
   "source": [
    "conn = duckdb.connect()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "900844e1-2b52-4789-9d5b-83f0ffa7c3db",
   "metadata": {},
   "outputs": [],
   "source": [
    "conn.execute(\"\"\"CREATE VIEW inputs_view AS\n",
    "SELECT address, SUM(t_value) AS t_value_sum, COUNT(*) AS count_\n",
    "FROM delta_table\n",
    "GROUP BY address\n",
    "\"\"\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "98f6ebbd-681b-4fd9-87d6-9d4463b82b0d",
   "metadata": {},
   "outputs": [],
   "source": [
    "%%time\n",
    "dataset = delta_table.to_pyarrow_dataset()  # !!! We SHOULD USE ARROW DATASET\n",
    "df = duckdb.arrow(dataset)\n",
    "query_from_delta = f\"SELECT address, max(t_value) as max_ FROM df WHERE partition_ <= 1 GROUP BY address ORDER BY max_ desc LIMIT 10;\"\n",
    "query_from_delta = \"\"\"select address, sum(t_value) as t_value, sum(enter_usdvalue) as enter_usdvalue,  \n",
    "                      count(*) as count_, max(t_time) as t_time from df WHERE partition_ <= 1 GROUP BY address ORDER BY t_value desc LIMIT 20;\"\"\"\n",
    "\n",
    "query_from_delta = f\"SELECT * FROM df WHERE partition_ == 1 LIMIT 10;\"\n",
    "df = duckdb.query(query_from_delta).pl()\n",
    "df"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "888f96e4-40c6-42e5-9810-a8b44ccffea7",
   "metadata": {},
   "outputs": [],
   "source": [
    "partition = 1\n",
    "df1 = get_data(partition, delta_table)\n",
    "df1"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "d1f8dba6-2ba1-4e2d-935b-d6ae9d658ec8",
   "metadata": {},
   "outputs": [],
   "source": [
    "def get_df(address, table_path): # когда надо получить все транзакции\n",
    "    partition = zlib.crc32(address.encode('utf-8')) % 1000\n",
    "    df = pl.scan_delta(table_path, \n",
    "      pyarrow_options={\n",
    "          \"partitions\": [(\"partition_\", \"=\", str(partition))]\n",
    "        }\n",
    "      )\n",
    "    df = df.filter(pl.col('address') == address)\n",
    "    return df.collect()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "a18639dd-db33-4570-9544-2d74dcce345b",
   "metadata": {},
   "outputs": [],
   "source": [
    "start_time = time.time()\n",
    "address = '112NjC8d5RQL81eTSb6472coeE4w9Qu92e'\n",
    "df = get_df(address, outputs_address_view)\n",
    "#df_in, df_out = await asyncio.gather(get_df_in(address, inputs_address_view), get_df_out(address, outputs_address_view))\n",
    "end_time = time.time()\n",
    "elapsed_time = end_time - start_time\n",
    "print(f\"Time taken: {elapsed_time} seconds\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "7b504bc3-d6b1-4c41-bd44-843fc332e389",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.13.2"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
