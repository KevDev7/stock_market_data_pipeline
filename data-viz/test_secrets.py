import streamlit as st

st.write("Snowflake account:", st.secrets["snowflake"]["account"])
st.write("Snowflake user:", st.secrets["snowflake"]["user"])