import io
import pandas as pd
import streamlit as st
from streamlit_pandas_profiling import st_profile_report
from streamlit_extras.metric_cards import style_metric_cards
from streamlit_extras.add_vertical_space import add_vertical_space
from tryout_phonepe import df_list

df_names = [var_name for var_name in globals()
            if isinstance(globals()[var_name], pd.core.frame.DataFrame) and var_name.endswith('_df')
            ]
# Initialize st.session_state if it's not already initialized
if df_list not in st.session_state:
    st.session_state['df_list'] = []
    for var_name in df_names:
        st.session_state[var_name] = globals()[var_name]
        st.session_state['df_list'].append(var_name)

# Reading from csv so as to make it work for everyone in streamlit cloud app...
# Otherwise there's another file named Home_with_SQL_Part.py in Miscellaneous directory in this same repo...

agg_trans_df = pd.read_csv(r'Miscellaneous/agg_trans.csv')
agg_user_df = pd.read_csv(r'Miscellaneous/agg_user.csv')
map_trans_df = pd.read_csv(r'Miscellaneous/map_trans.csv')
map_user_df = pd.read_csv(r'Miscellaneous/map_user.csv')
top_trans_dist_df = pd.read_csv(r'Miscellaneous/top_trans_dist.csv')
top_trans_pin_df = pd.read_csv(r'Miscellaneous/top_trans_pin.csv')
top_user_dist_df = pd.read_csv(r'Miscellaneous/top_user_dist.csv')
top_user_pin_df = pd.read_csv(r'Miscellaneous/top_user_pin.csv')

if 'options' not in st.session_state:
    st.session_state['options'] = {
        'Aggregate Transaction': 'agg_trans_df',
        'Aggregate User': 'agg_user_df',
        'Map Transaction': 'map_trans_df',
        'Map User': 'map_user_df',
        'Top Transaction Districtwise': 'top_trans_dist_df',
        'Top Transaction Pincodewise': 'top_trans_pin_df',
        'Top User Districtwise': 'top_user_dist_df',
        'Top User Pincodewise': 'top_user_pin_df'
    }

# def year_to_str(df):
#     df['Year'] = df["Year"].astype(str)
#
#
# for df_name in st.session_state['df_list']:
#     df = globals()[df_name]
#     year_to_str(df)
#     globals()[df_name] = df

# App


st.set_page_config(
    page_title='PhonePe Data Visualization', layout='wide',
    page_icon='Related Images and Videos/Logo.png'
)

st.title(':voilet[PhonePe Data Visualization]')

add_vertical_space(2)

phonepe_description = """PhonePe has launched PhonePe Pulse, a data analytics platform that offers valuable insights and 
                        data related to digital transactions made through the PhonePe platform. It provides a comprehensive 
                        overview of transaction trends, user behavior, and market insights. With PhonePe Pulse, 
                        businesses and analysts can access real-time transaction data, understand customer preferences, 
                        track transaction volumes, and gain actionable insights to make informed business decisions. 
                        This powerful tool helps businesses optimize their strategies, identify growth opportunities, 
                        and stay ahead in the digital payments landscape. By leveraging the rich data and analytics 
                        provided by PhonePe Pulse, businesses can enhance their understanding of consumer behavior, 
                        tailor their offerings, and drive growth in the digital payments ecosystem."""

st.write(phonepe_description)

add_vertical_space(2)

st.write(':green[Digital Transaction Penetration has Grown Unevenly Across States; '
         'Higher Penetration in Southern and Western India]')

add_vertical_space(2)

st.image('Related Images/trans_penetration.png')

add_vertical_space(2)

col1, col2, col3 = st.columns(3)

total_reg_users = top_user_dist_df['Registered_users'].sum()
col1.metric(
    label='Total Registered Users',
    value='{:.2f} Cr'.format(total_reg_users / 100000000),
    delta='Forward Trend'
)

total_app_opens = map_user_df['App_opens'].sum()
col2.metric(
    label='Total App Opens', value='{:.2f} Cr'.format(total_app_opens / 100000000),
    delta='Forward Trend'
)

col3.metric(label='Total Transaction Count', value='2000 Cr +', delta='Forward Trend')

style_metric_cards(background_color='200329')

add_vertical_space(2)

st.image('Related Images/Pulse.gif', use_column_width=True)

add_vertical_space(2)

col, buff = st.columns([2, 4])

option = col.selectbox(
    label='Select Dataset',
    options=list(st.session_state['options'].keys()),
    key='df'
)

tab1, tab2 = st.tabs(['Report and Dataset', 'Download Dataset'])

with tab1:
    column1, column2, buffer = st.columns([2, 2, 4])

    show_profile = column1.button(label='Show Detailed Report', key='show')
    show_df = column2.button(label='Show Dataset', key='show_df')

    if show_profile:
        df_name = st.session_state['options'][option]
        df = globals()[df_name]
        pr = df.profile_report()
        st_profile_report(pr)

    if show_df:
        st.experimental_data_editor(
            data=globals()[st.session_state['options'][option]],
            use_container_width=True
        )

with tab2:
    col1, col2, col3 = st.columns(3)

    df_name = st.session_state['options'][option]
    df = globals()[df_name]

    csv = df.to_csv()
    json = df.to_json(orient='records')
    #creating in-memory bytes buffer
    excel_buffer = io.BytesIO()
    #write contents of dataframe to excel_buffer using 'to_excel' method
    df.to_excel(excel_buffer, engine='xlsxwriter', index=False)
    #retrieve bytes representing excel-file data
    excel_bytes = excel_buffer.getvalue()
    #creating download buttons for csv,json,excel files
    #key is the unique identifier to the respective download buttons
    col1.download_button(
        "Download CSV file", data=csv,
        file_name=f'{option}.csv',
        mime='text/csv', key='csv'
    )
    col2.download_button(
        "Download JSON file", data=json,
        file_name=f'{option}.json',
        mime='application/json', key='json'
    )
    col3.download_button("Download Excel file", data=excel_bytes,
                         file_name=f'{option}.xlsx',
                         mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                         key='excel'
                         )