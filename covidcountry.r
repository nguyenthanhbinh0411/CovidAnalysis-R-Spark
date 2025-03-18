library(shiny)
library(ggplot2)
library(dplyr)
library(sparklyr)
library(factoextra)

# Kết nối Spark
spark_disconnect_all()
sc <- spark_connect(master = "local")

# Đọc dữ liệu từ CSV vào Spark DataFrame
data_path <- "C:/Users/Admin/Downloads/covid_19_data.csv"
covid_data <- spark_read_csv(sc, name = "covid", path = data_path, 
                             infer_schema = TRUE, header = TRUE)

# Kiểm tra định dạng của ObservationDate trước khi chuyển đổi
covid_data <- covid_data %>%
  mutate(ObservationDate = to_date(ObservationDate, "MM/dd/yyyy")) %>%
  mutate(Country = trimws(CountryRegion)) %>%
  select(ObservationDate, Country, Confirmed, Deaths, Recovered)

# Tổng hợp dữ liệu ngay trên Spark
covid_summary_spark <- covid_data %>%
  group_by(ObservationDate, Country) %>%
  summarise(
    Total_Confirmed = sum(Confirmed, na.rm = TRUE),
    Total_Deaths = sum(Deaths, na.rm = TRUE),
    Total_Recovered = sum(Recovered, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  compute(name = "covid_summary") # Lưu kết quả tạm thời trên Spark

# Thu thập dữ liệu từ Spark về R (chỉ khi cần vẽ biểu đồ)
covid_summary <- covid_summary_spark %>% collect()

# Giao diện web
ui <- fluidPage(
  titlePanel("Phân tích COVID-19"),
  
  sidebarLayout(
    sidebarPanel(
      selectInput("country", "Chọn quốc gia:", 
                  choices = unique(covid_summary$Country),
                  selected = "Vietnam"),
      actionButton("update", "Cập nhật biểu đồ")
    ),
    
    mainPanel(
      plotOutput("timeSeriesPlot")
    )
  )
)

# Xử lý logic server
server <- function(input, output) {
  
  observeEvent(input$update, {
    
    filtered_data <- covid_summary %>%
      filter(Country == input$country)
    
    # Biểu đồ đường hiển thị số ca nhiễm, tử vong và hồi phục
    output$timeSeriesPlot <- renderPlot({
      ggplot(filtered_data, aes(x = ObservationDate)) +
        geom_line(aes(y = Total_Confirmed, color = "Nhiễm"), size = 1) +
        geom_line(aes(y = Total_Deaths, color = "Tử vong"), size = 1) +
        geom_line(aes(y = Total_Recovered, color = "Hồi phục"), size = 1) +
        scale_color_manual(values = c("Nhiễm" = "blue", "Tử vong" = "red", "Hồi phục" = "green")) +
        labs(title = paste("Diễn biến COVID-19 tại", input$country),
             x = "Ngày", y = "Số ca", color = "Loại ca") +
        theme_minimal()
    })
  })
}

# Chạy ứng dụng Shiny
shinyApp(ui = ui, server = server)
