# Load các thư viện cần thiết
library(sparklyr)
library(dplyr)
library(ggplot2)
library(cluster)
library(factoextra)
library(forecast)
library(tidyr)
library(scales)
library(lubridate)
library(pheatmap)
library(countrycode)

# 1. Kết nối đến Spark (chạy ở chế độ local)
spark_disconnect_all()
if (!"3.4.1" %in% spark_available_versions()) {
  spark_install(version = "3.4.1")
}
sc <- spark_connect(master = "local")

# 2. Đẩy dữ liệu lên Spark từ file CSV
data_path <- "C:/Users/Admin/Downloads/covid_19_data.csv"
covid_data <- spark_read_csv(sc, name = "covid", path = data_path, 
                             infer_schema = TRUE, header = TRUE)
glimpse(covid_data)

# (Nếu có dữ liệu R và muốn đẩy lên Spark, sử dụng copy_to như sau)
# local_df <- read.csv(data_path, header = TRUE, stringsAsFactors = FALSE)
# spark_df <- copy_to(sc, local_df, "spark_covid", overwrite = TRUE)
# glimpse(spark_df)

# 3. Tiền xử lý dữ liệu trên Spark
covid_cleaned <- covid_data %>%
  mutate(
    ObservationDate = to_timestamp(ObservationDate, "MM/dd/yyyy"),  # Chuyển đổi ngày sang timestamp
    Country = trimws(CountryRegion),                                # Loại bỏ khoảng trắng thừa
    Province = ifelse(is.na(ProvinceState) | ProvinceState == "", "Unknown", ProvinceState)
  ) %>%
  select(-Last_Update)  # Loại bỏ cột không cần thiết

# 4. Tổng hợp số ca theo quốc gia và ngày trên Spark
covid_summary_spark <- covid_cleaned %>%
  group_by(ObservationDate, Country) %>%
  summarise(
    Total_Confirmed = sum(Confirmed, na.rm = TRUE),
    Total_Deaths = sum(Deaths, na.rm = TRUE),
    Total_Recovered = sum(Recovered, na.rm = TRUE)
  )

# Thu thập dữ liệu từ Spark về R để thực hiện các phân tích tiếp theo
covid_summary <- covid_summary_spark %>% collect()

# 5. Thống kê tổng quan trên dữ liệu (trên R)
covid_summary <- covid_summary %>%
  mutate(
    Fatality_Rate = ifelse(Total_Confirmed > 0, Total_Deaths / Total_Confirmed, 0),
    Recovery_Rate = ifelse(Total_Confirmed > 0, Total_Recovered / Total_Confirmed, 0)
  )

# 6. Phân tích cụm (Clustering) trực tiếp trên Spark

# 6.1. Xác định ngày quan sát mới nhất (tính trên Spark)
max_date <- covid_cleaned %>%
  summarise(max_date = max(ObservationDate)) %>%
  collect() %>%
  .$max_date

# 6.2. Lấy dữ liệu mới nhất theo từng quốc gia (trên Spark)
covid_latest_spark <- covid_cleaned %>%
  filter(ObservationDate == max_date) %>%
  group_by(Country) %>%
  summarise(
    Total_Confirmed = sum(Confirmed, na.rm = TRUE),
    Total_Deaths = sum(Deaths, na.rm = TRUE),
    Total_Recovered = sum(Recovered, na.rm = TRUE)
  )

# 6.3. Chuẩn bị dữ liệu cho mô hình MLlib: kết hợp các cột số liệu thành vector đặc trưng
# Đổi tên cột output thành "input_features" để tránh xung đột
covid_features <- covid_latest_spark %>%
  ft_vector_assembler(
    input_cols = c("Total_Confirmed", "Total_Deaths", "Total_Recovered"),
    output_col = "input_features"
  )

# 6.4. Áp dụng mô hình K-Means trên Spark với số cụm k = 3
set.seed(123)
k <- 3
kmeans_model_spark <- covid_features %>%
  ml_kmeans(~ input_features, k = k, seed = 123)

# Dự đoán nhãn cụm và thêm cột 'prediction'
covid_clustered_spark <- ml_predict(kmeans_model_spark, covid_features)

# Thu thập dữ liệu kết quả phân cụm về R để trực quan hóa
covid_clustered_local <- covid_clustered_spark %>%
  select(Country, Total_Confirmed, Total_Deaths, Total_Recovered, prediction) %>%
  collect()
print(covid_clustered_local)

# 7. Tiếp tục phân tích và trực quan hóa trên R

## 7.1. Phân tích cụm bằng R (Elbow, trực quan hóa, thống kê tổng số ca theo cụm)
covid_latest <- covid_summary %>%
  filter(ObservationDate == max(ObservationDate)) %>%
  select(Country, Total_Confirmed, Total_Deaths, Total_Recovered) %>%
  drop_na()

covid_scaled <- covid_latest %>%
  select(-Country) %>%
  scale()

wcss <- vector()
for (k in 1:10) {
  kmeans_result <- kmeans(covid_scaled, centers = k, nstart = 10)
  wcss[k] <- kmeans_result$tot.withinss
}
plot(1:10, wcss, type = "b", pch = 19, frame = FALSE,
     xlab = "Số cụm K",
     ylab = "Tổng phương sai nội bộ (WCSS)",
     main = "Phương pháp Elbow để xác định số cụm tối ưu")
optimal_k <- which(diff(diff(wcss)) == min(diff(diff(wcss)))) + 1

set.seed(123)
kmeans_result <- kmeans(covid_scaled, centers = 3, nstart = 10)
covid_latest$Cluster <- as.factor(kmeans_result$cluster)

cluster_colors <- c("1" = "green", "2" = "red", "3" = "blue")

fviz_cluster(kmeans_result, data = covid_scaled, geom = "point", ellipse.type = "convex") +
  scale_color_manual(values = cluster_colors, aesthetics = "colour") +
  scale_fill_manual(values = cluster_colors, aesthetics = "fill") +
  labs(title = "Kết quả phân cụm các quốc gia theo mức độ ảnh hưởng dịch COVID-19") +
  theme_minimal() +
  theme(legend.position = "top")

cluster_cases <- covid_latest %>%
  group_by(Cluster) %>%
  summarise(Total_Cases = sum(Total_Confirmed)) %>%
  mutate(Percentage = Total_Cases / sum(Total_Cases) * 100)

ggplot(cluster_cases, aes(x = Cluster, y = Percentage, fill = Cluster)) +
  geom_bar(stat = "identity", color = "black", width = 0.6) +
  scale_fill_manual(values = cluster_colors) +
  geom_text(aes(label = paste0(round(Percentage, 1), "%\n(", format(Total_Cases, big.mark = "."), " ca)")), 
            vjust = -0.3, size = 4) +
  ylim(0, max(cluster_cases$Percentage) + 5) +
  labs(title = "Tỷ lệ phần trăm ca nhiễm của các cụm",
       x = "Cụm", y = "Phần trăm ca nhiễm (%)") +
  theme_minimal()

## 7.2. Trực quan hóa sự phát triển của số ca nhiễm theo thời gian
ggplot(covid_summary, aes(x = ObservationDate, y = Total_Confirmed)) +
  geom_line(color = "blue", size = 1) +
  labs(
    title = "Sự phát triển của số ca nhiễm COVID-19 theo thời gian",
    x = "Ngày", y = "Số ca nhiễm",
    subtitle = "Biểu đồ đường thể hiện tổng số ca nhiễm theo thời gian"
  ) +
  theme_minimal() +
  scale_y_continuous(labels = scales::label_comma()) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

## 7.3. Biểu đồ phân tán số ca nhiễm và tử vong theo cụm
covid_latest <- covid_latest %>%
  mutate(
    Total_Confirmed = ifelse(Total_Confirmed == 0, 1, Total_Confirmed),
    Total_Deaths = ifelse(Total_Deaths == 0, 1, Total_Deaths)
  )
ggplot(covid_latest, aes(x = Total_Confirmed, y = Total_Deaths, color = as.factor(Cluster))) +
  geom_point(size = 4, alpha = 0.7) +
  scale_x_log10(labels = label_comma()) +
  scale_y_log10(labels = label_comma()) +
  scale_color_manual(values = cluster_colors) +
  theme_minimal() +
  labs(title = "Mối quan hệ giữa số ca nhiễm và số ca tử vong",
       x = "Số ca nhiễm (log)",
       y = "Số ca tử vong (log)",
       color = "Nhóm (Cluster)") +
  theme(legend.position = "top")

## 7.4. Heatmap mức độ tập trung dịch bệnh
top_countries <- covid_summary %>%
  group_by(Country) %>%
  summarise(Total_Confirmed = sum(Total_Confirmed, na.rm = TRUE),
            Total_Deaths = sum(Total_Deaths, na.rm = TRUE)) %>%
  arrange(desc(Total_Confirmed))
top_10_countries <- top_countries %>% slice_head(n = 10) %>% pull(Country)
top_10_death_countries <- top_countries %>% arrange(desc(Total_Deaths)) %>% slice_head(n = 10) %>% pull(Country)
daily_totals <- covid_summary %>%
  group_by(ObservationDate) %>%
  summarise(Total_Confirmed = sum(Total_Confirmed, na.rm = TRUE),
            Total_Deaths = sum(Total_Deaths, na.rm = TRUE))

plot_heatmap <- function(selected_countries, title, value_column, total_column) {
  covid_wide <- covid_summary %>%
    filter(Country %in% selected_countries) %>%
    select(ObservationDate, Country, all_of(value_column)) %>%
    pivot_wider(names_from = Country, 
                values_from = all_of(value_column), 
                values_fill = setNames(list(0), value_column))
  
  covid_wide <- covid_wide %>%
    left_join(daily_totals %>% select(ObservationDate, all_of(total_column)), by = "ObservationDate")
  
  covid_matrix <- as.matrix(covid_wide[, -1])
  formatted_dates <- as.Date(covid_wide$ObservationDate)
  
  heatmap(covid_matrix, Rowv = NA, Colv = NA, col = rev(heat.colors(256)), scale = "row",
          main = title, xlab = "Quốc gia", ylab = "Thời gian",
          cexCol = 0.7, cexRow = 0.7, margins = c(8, 8),
          labRow = format(formatted_dates, "%d-%m-%Y"))
}

# Vẽ heatmap cho 10 quốc gia có số ca mắc cao nhất
plot_heatmap(c(top_10_countries, "Total_Confirmed"), "Heatmap - 10 quốc gia có số ca mắc cao nhất", "Total_Confirmed", "Total_Confirmed")
# Và heatmap cho 10 quốc gia có số ca tử vong cao nhất
plot_heatmap(c(top_10_death_countries, "Total_Deaths"), "Heatmap - 10 quốc gia có số ca tử vong cao nhất", "Total_Deaths", "Total_Deaths")

## 7.5. Biểu đồ hiển thị tổng số ca mắc, hồi phục và tử vong của 10 quốc gia có số ca mắc cao nhất
latest_data <- covid_summary %>%
  group_by(Country) %>%
  filter(ObservationDate == max(ObservationDate)) %>%
  ungroup()
top_countries <- latest_data %>%
  arrange(desc(Total_Confirmed)) %>%
  head(10)
start_date <- min(covid_summary$ObservationDate, na.rm = TRUE)
end_date <- max(covid_summary$ObservationDate, na.rm = TRUE)
top_countries_long <- top_countries %>%
  pivot_longer(cols = c(Total_Confirmed, Total_Recovered, Total_Deaths), 
               names_to = "Case_Type", values_to = "Count")
ggplot(top_countries_long, aes(x = reorder(Country, -Count), y = Count, fill = Case_Type)) +
  geom_bar(stat = "identity", position = "dodge") +
  scale_fill_manual(values = c("Total_Confirmed" = "blue", "Total_Recovered" = "green", "Total_Deaths" = "red")) +
  labs(title = paste("Tổng số ca mắc, hồi phục và tử vong của 10 quốc gia có số ca mắc cao nhất (",
                     format(start_date, "%d-%m-%Y"), " đến ", format(end_date, "%d-%m-%Y"), ")"),
       x = "Quốc gia", y = "Số ca", fill = "Loại ca") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  scale_y_continuous(labels = scales::comma)

## 7.6. Biểu đồ hiển thị tổng số ca mắc, hồi phục và tử vong của 10 quốc gia có số ca mắc thấp nhất
bottom_countries <- covid_summary %>%
  group_by(Country) %>%
  summarise(Total_Confirmed = sum(Total_Confirmed),
            Total_Recovered = sum(Total_Recovered),
            Total_Deaths = sum(Total_Deaths)) %>%
  filter(Total_Confirmed > 0) %>%
  arrange(Total_Confirmed) %>%
  head(10)
bottom_countries_long <- bottom_countries %>%
  pivot_longer(cols = c(Total_Confirmed, Total_Recovered, Total_Deaths), 
               names_to = "Case_Type", values_to = "Count")
ggplot(bottom_countries_long, aes(x = reorder(Country, -Count), y = Count, fill = Case_Type)) +
  geom_bar(stat = "identity", position = "dodge") +
  scale_fill_manual(values = c("Total_Confirmed" = "blue", "Total_Recovered" = "green", "Total_Deaths" = "red")) +
  labs(title = paste("Tổng số ca mắc, hồi phục và tử vong của 10 quốc gia có số ca mắc thấp nhất (",
                     format(start_date, "%d-%m-%Y"), " đến ", format(end_date, "%d-%m-%Y"), ")"),
       x = "Quốc gia", y = "Số ca", fill = "Loại ca") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  scale_y_continuous(labels = scales::comma)

## 7.7. Biểu đồ đường so sánh số ca nhiễm và hồi phục theo châu lục
# Xử lý tên quốc gia không khớp: chuyển một số giá trị không rõ ràng thành NA hoặc thay thế theo ý bạn
covid_summary <- covid_summary %>%
  mutate(Country = case_when(
    Country %in% c("Channel Islands", "Diamond Princess", "Kosovo", 
                   "Micronesia", "MS Zaandam", "North Ireland", "Others", "St. Martin") ~ NA_character_,
    TRUE ~ Country
  )) %>%
  mutate(Continent = countrycode(Country, origin = "country.name", destination = "continent"))

continent_summary <- covid_summary %>%
  group_by(ObservationDate, Continent) %>%
  summarise(Total_Confirmed = sum(Total_Confirmed, na.rm = TRUE),
            Total_Recovered = sum(Total_Recovered, na.rm = TRUE)) %>%
  drop_na()

ggplot(continent_summary, aes(x = ObservationDate)) +
  geom_line(aes(y = Total_Confirmed, color = Continent), size = 1) +
  geom_line(aes(y = Total_Recovered, color = Continent), size = 1, linetype = "dashed") +
  labs(title = "Tổng số ca nhiễm và hồi phục theo châu lục",
       x = "Ngày", y = "Số ca", color = "Châu lục") +
  scale_y_continuous(labels = scales::comma) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

## 7.8. Biểu đồ nhiệt so sánh số ca nhiễm và hồi phục theo châu lục
continent_long <- continent_summary %>%
  pivot_longer(cols = c(Total_Confirmed, Total_Recovered), 
               names_to = "Type", values_to = "Count") %>%
  mutate(ObservationDate = as.Date(ObservationDate))
continent_wide <- continent_long %>%
  pivot_wider(names_from = Continent, values_from = Count, values_fill = list(Count = 0)) %>%
  arrange(ObservationDate)
continent_matrix <- as.matrix(continent_wide[, -1])
continent_matrix <- apply(continent_matrix, 2, as.numeric)
formatted_dates <- as.Date(continent_wide$ObservationDate)
if (nrow(continent_matrix) > 1 && ncol(continent_matrix) > 1) {
  heatmap(continent_matrix, Rowv = NA, Colv = NA, 
          col = rev(heat.colors(256)), scale = "none", 
          main = "Biểu đồ nhiệt số ca nhiễm và hồi phục theo châu lục", 
          xlab = "Châu lục", ylab = "Thời gian", 
          margins = c(8, 8), cexRow = 1, cexCol = 0.7,
          labRow = format(formatted_dates, "%d-%m-%Y"))
} else {
  print("Dữ liệu không đủ để tạo biểu đồ nhiệt.")
}

## 7.9. Dự báo xu hướng lây nhiễm COVID-19 bằng mô hình ARIMA
daily_cases <- covid_summary %>%
  group_by(ObservationDate) %>%
  summarise(Total_Confirmed = sum(Total_Confirmed, na.rm = TRUE)) %>%
  arrange(ObservationDate)
ts_cases <- ts(daily_cases$Total_Confirmed, frequency = 7)
adf_test <- tseries::adf.test(ts_cases)
print(adf_test)
if (adf_test$p.value > 0.05) {
  ts_cases_diff <- diff(ts_cases)
} else {
  ts_cases_diff <- ts_cases
}
arima_model <- auto.arima(ts_cases_diff)
print(arima_model)
forecast_result <- forecast::forecast(arima_model, h = 30)
forecast::autoplot(forecast_result) +
  labs(
    title = "Dự báo xu hướng lây nhiễm COVID-19 (30 ngày tiếp theo)",
    x = "Ngày", y = "Số ca nhiễm",
    subtitle = "Dự báo dựa trên mô hình ARIMA"
  ) +
  theme_minimal()

# 8. Ngắt kết nối Spark khi hoàn tất
spark_disconnect(sc)
