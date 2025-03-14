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

# 2. Kết nối Spark (chạy ở chế độ local)
spark_disconnect_all()

if (!"3.4.1" %in% spark_available_versions()) {
  spark_install(version = "3.4.1")
}

sc <- spark_connect(master = "local")

# 3. Tải dữ liệu từ CSV (đảm bảo đường dẫn đúng)
data_path <- "covid_19_data.csv"

covid_data <- spark_read_csv(sc, name = "covid", path = data_path, 
                             infer_schema = TRUE, header = TRUE)

# Kiểm tra dữ liệu
glimpse(covid_data)

# 4. Tiền xử lý dữ liệu
covid_cleaned <- covid_data %>%
  mutate(
    ObservationDate = to_timestamp(ObservationDate, "MM/dd/yyyy"),  # Chuyển đổi ngày từ chuỗi sang kiểu timestamp
    Country = trimws(CountryRegion),                                # Loại bỏ khoảng trắng dư thừa trong tên quốc gia
    Province = ifelse(is.na(ProvinceState) | ProvinceState == "", "Unknown", ProvinceState)  # Thay thế giá trị NA hoặc rỗng bằng "Unknown"
  ) %>%
  select(-Last_Update)  # Loại bỏ cột 'Last_Update' vì không cần thiết cho phân tích

# Tổng hợp số ca theo quốc gia và ngày
covid_summary <- covid_cleaned %>%
  group_by(ObservationDate, Country) %>%
  summarise(
    Total_Confirmed = sum(Confirmed, na.rm = TRUE),
    Total_Deaths = sum(Deaths, na.rm = TRUE),
    Total_Recovered = sum(Recovered, na.rm = TRUE)
  ) %>%
  collect()

# 5. Phân tích dữ liệu

# 5.1. Thống kê tổng quan
covid_summary <- covid_summary %>%
  mutate(
    Fatality_Rate = ifelse(Total_Confirmed > 0, Total_Deaths / Total_Confirmed, 0),
    Recovery_Rate = ifelse(Total_Confirmed > 0, Total_Recovered / Total_Confirmed, 0)
  )

# 5.2. Phân tích cụm (Clustering) theo mức độ ảnh hưởng

# 5.2.1. Xác định số cụm tối ưu bằng phương pháp Elbow
wcss <- vector()
for (k in 1:10) {
  kmeans_result <- kmeans(covid_scaled, centers = k, nstart = 10)
  wcss[k] <- kmeans_result$tot.withinss
}

# Vẽ đồ thị WCSS theo số cụm k
plot(1:10, wcss, type = "b", pch = 19, frame = FALSE,
     xlab = "Số cụm K",
     ylab = "Tổng phương sai nội bộ (WCSS)",
     main = "Phương pháp Elbow để xác định số cụm tối ưu")

# 5.2.2. Phân cụm dữ liệu
covid_latest <- covid_summary %>%
  filter(ObservationDate == max(ObservationDate)) %>%
  select(Country, Total_Confirmed, Total_Deaths, Total_Recovered)

# Chuẩn hóa dữ liệu trước khi phân cụm
covid_scaled <- covid_latest %>%
  select(-Country) %>%
  scale()

# Áp dụng K-Means để phân cụm
set.seed(123)
kmeans_result <- kmeans(covid_scaled, centers = 3, nstart = 10)

# Thêm nhãn cụm vào dữ liệu
covid_latest$Cluster <- as.factor(kmeans_result$cluster)

# Xác định màu sắc theo mức độ ảnh hưởng
cluster_colors <- c("1" = "green",  
                    "2" = "red",   
                    "3" = "blue")  

# Trực quan hóa kết quả phân cụm với màu tùy chỉnh
fviz_cluster(kmeans_result, data = covid_scaled, geom = "point", ellipse.type = "convex") +
  scale_color_manual(values = cluster_colors, aesthetics = "colour") +  # Gán màu đúng
  scale_fill_manual(values = cluster_colors, aesthetics = "fill") +  # Đảm bảo màu cũng áp dụng cho fill
  labs(title = "Kết quả phân cụm các quốc gia theo mức độ ảnh hưởng dịch COVID-19") +
  theme_minimal() +
  theme(legend.position = "top")

# Tính tổng số ca nhiễm trong mỗi cụm
cluster_cases <- covid_latest %>%
  group_by(Cluster) %>%
  summarise(Total_Cases = sum(Total_Confirmed)) %>%
  mutate(Percentage = Total_Cases / sum(Total_Cases) * 100)  # Tính phần trăm

# Vẽ biểu đồ cột thể hiện phần trăm ca nhiễm của từng cụm
ggplot(cluster_cases, aes(x = Cluster, y = Percentage, fill = Cluster)) +
  geom_bar(stat = "identity", color = "black", width = 0.6) +  # Thu nhỏ chiều rộng cột
  scale_fill_manual(values = cluster_colors) +  # Áp dụng màu sắc theo cụm
  geom_text(aes(label = paste0(round(Percentage, 1), "%\n(", format(Total_Cases, big.mark = "."), " ca)")), 
            vjust = -0.3, size = 4) +  # Giảm size chữ để vừa vặn
  ylim(0, max(cluster_cases$Percentage) + 5) +  # Điều chỉnh trục y để nhãn không bị cắt
  labs(title = "Tỷ lệ phần trăm ca nhiễm của các cụm",
       x = "Cụm", y = "Phần trăm ca nhiễm (%)") +
  theme_minimal()


#5.3. Trực quan hóa sự phát triển của số ca nhiễm theo thời gian
ggplot(covid_summary, aes(x = ObservationDate, y = Total_Confirmed)) +
  geom_line(color = "blue", size = 1) +  # Biểu đồ đường với màu xanh dương
  labs(
    title = "Sự phát triển của số ca nhiễm COVID-19 theo thời gian",
    x = "Ngày", y = "Số ca nhiễm",
    subtitle = "Biểu đồ đường thể hiện tổng số ca nhiễm theo thời gian"
  ) +
  theme_minimal() +  # Giao diện tối giản
  scale_y_continuous(labels = scales::label_comma()) +  # Hiển thị số theo định dạng thông thường
  theme(axis.text.x = element_text(angle = 45, hjust = 1))  # Xoay nhãn trục X cho dễ đọc




# 6. Trực quan hóa dữ liệu
# 6.1. Biểu đồ phân tán số ca nhiễm và tử vong
# Định nghĩa màu sắc cho từng cluster
cluster_colors <- c("1" = "green",  
                    "2" = "red",   
                    "3" = "blue")  

# Cập nhật dữ liệu để tránh lỗi giá trị 0
covid_latest <- covid_latest %>%
  mutate(
    Total_Confirmed = ifelse(Total_Confirmed == 0, 1, Total_Confirmed),
    Total_Deaths = ifelse(Total_Deaths == 0, 1, Total_Deaths)
  )

# Vẽ biểu đồ với màu tùy chỉnh
ggplot(covid_latest, aes(x = Total_Confirmed, y = Total_Deaths, color = as.factor(Cluster))) +
  geom_point(size = 4, alpha = 0.7) +  # Thêm kích thước và độ trong suốt cho các điểm
  scale_x_log10(labels = label_comma()) +  # Hiển thị số dạng thông thường trên trục x
  scale_y_log10(labels = label_comma()) +  # Hiển thị số dạng thông thường trên trục y
  scale_color_manual(values = cluster_colors) +  # Áp dụng màu sắc tùy chỉnh
  theme_minimal() +
  labs(title = "Mối quan hệ giữa số ca nhiễm và số ca tử vong",
       x = "Số ca nhiễm (log)",
       y = "Số ca tử vong (log)",
       color = "Nhóm (Cluster)") +  # Đổi tên chú thích màu
  theme(legend.position = "top")

# 6.2. Heatmap mức độ tập trung dịch bệnh
# Tính tổng số ca nhiễm theo quốc gia
top_countries <- covid_summary %>%
  group_by(Country) %>%
  summarise(Total_Confirmed = sum(Total_Confirmed, na.rm = TRUE),
            Total_Deaths = sum(Total_Deaths, na.rm = TRUE)) %>%
  arrange(desc(Total_Confirmed))

# Lấy 10 quốc gia có số ca nhiễm cao nhất
top_10_countries <- top_countries %>%
  slice_head(n = 10) %>%
  pull(Country)

# Lấy 10 quốc gia có số ca tử vong cao nhất
top_10_death_countries <- top_countries %>%
  arrange(desc(Total_Deaths)) %>%
  slice_head(n = 10) %>%
  pull(Country)

# Tính tổng số ca mắc và tử vong theo ngày
daily_totals <- covid_summary %>%
  group_by(ObservationDate) %>%
  summarise(Total_Confirmed = sum(Total_Confirmed, na.rm = TRUE),
            Total_Deaths = sum(Total_Deaths, na.rm = TRUE))

# Hàm tạo heatmap
plot_heatmap <- function(selected_countries, title, value_column, total_column) {
  covid_wide <- covid_summary %>%
    filter(Country %in% selected_countries) %>%
    select(ObservationDate, Country, all_of(value_column)) %>%
    pivot_wider(names_from = Country, values_from = all_of(value_column), values_fill = list(value_column = 0))
  
  # Ghép thêm cột tổng số ca mắc/tử vong theo ngày
  covid_wide <- covid_wide %>%
    left_join(daily_totals %>% select(ObservationDate, all_of(total_column)), by = "ObservationDate")
  
  # Đảm bảo chỉ lấy đúng dữ liệu số (bỏ ObservationDate)
  covid_matrix <- as.matrix(covid_wide[, -1])
  formatted_dates <- as.Date(covid_wide$ObservationDate)
  
  heatmap(covid_matrix, Rowv = NA, Colv = NA, col = rev(heat.colors(256)), scale = "row",
          main = title,
          xlab = "Quốc gia", ylab = "Thời gian",
          cexCol = 0.7, cexRow = 0.7,
          margins = c(8, 8),
          labRow = format(formatted_dates, "%d-%m-%Y"))
}

# Vẽ heatmap cho 10 quốc gia có số ca mắc cao nhất + tổng số ca mắc theo ngày
plot_heatmap(c(top_10_countries, "Total_Confirmed"), "Heatmap - 10 quốc gia có số ca mắc cao nhất", "Total_Confirmed", "Total_Confirmed")

# Vẽ heatmap cho 10 quốc gia có số ca tử vong cao nhất + tổng số ca tử vong theo ngày
plot_heatmap(c(top_10_death_countries, "Total_Deaths"), "Heatmap - 10 quốc gia có số ca tử vong cao nhất", "Total_Deaths", "Total_Deaths")

# 6.3. Biểu đồ hiển thị tổng số ca mắc, hồi phục và tử vong của 10 quốc gia có số ca mắc cao nhất

# Lấy dữ liệu mới nhất theo từng quốc gia
latest_data <- covid_summary %>%
  group_by(Country) %>%
  filter(ObservationDate == max(ObservationDate)) %>%  # Chỉ lấy dữ liệu ngày mới nhất
  ungroup()

# Chọn 10 quốc gia có số ca mắc cao nhất
top_countries <- latest_data %>%
  arrange(desc(Total_Confirmed)) %>%
  head(10)

# Lấy ngày đầu tiên và ngày cuối cùng trong dữ liệu
start_date <- min(covid_summary$ObservationDate, na.rm = TRUE)
end_date <- max(covid_summary$ObservationDate, na.rm = TRUE)

# Chuyển đổi dữ liệu sang dạng phù hợp cho ggplot
top_countries_long <- top_countries %>%
  pivot_longer(cols = c(Total_Confirmed, Total_Recovered, Total_Deaths), 
               names_to = "Case_Type", 
               values_to = "Count")

# Vẽ biểu đồ cột
ggplot(top_countries_long, aes(x = reorder(Country, -Count), y = Count, fill = Case_Type)) +
  geom_bar(stat = "identity", position = "dodge") +
  scale_fill_manual(values = c("Total_Confirmed" = "blue", "Total_Recovered" = "green", "Total_Deaths" = "red")) +
  labs(title = paste("Tổng số ca mắc, hồi phục và tử vong của 10 quốc gia có số ca mắc cao nhất (",
                     format(start_date, "%d-%m-%Y"), " đến ", format(end_date, "%d-%m-%Y"), ")"),
       x = "Quốc gia", 
       y = "Số ca",
       fill = "Loại ca") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  scale_y_continuous(labels = scales::comma)


# 6.4. Biểu đồ hiển thị tổng số ca mắc, hồi phục và tử vong của 10 quốc gia có số ca mắc thấp nhất

# Lấy danh sách 10 quốc gia có số ca mắc thấp nhất (loại bỏ những quốc gia có 0 ca)
bottom_countries <- covid_summary %>%
  group_by(Country) %>%
  summarise(
    Total_Confirmed = sum(Total_Confirmed),
    Total_Recovered = sum(Total_Recovered),
    Total_Deaths = sum(Total_Deaths)  # Thêm cột số ca tử vong
  ) %>%
  filter(Total_Confirmed > 0) %>%
  arrange(Total_Confirmed) %>%
  head(10)  # Hiển thị 10 quốc gia thay vì 5

# Chuyển đổi dữ liệu sang dạng phù hợp cho ggplot
bottom_countries_long <- bottom_countries %>%
  pivot_longer(cols = c(Total_Confirmed, Total_Recovered, Total_Deaths), 
               names_to = "Case_Type", 
               values_to = "Count")

# Vẽ biểu đồ cột
ggplot(bottom_countries_long, aes(x = reorder(Country, -Count), y = Count, fill = Case_Type)) +
  geom_bar(stat = "identity", position = "dodge") +
  scale_fill_manual(values = c("Total_Confirmed" = "blue", "Total_Recovered" = "green", "Total_Deaths" = "red")) +
  labs(title = paste("Tổng số ca mắc, hồi phục và tử vong của 10 quốc gia có số ca mắc thấp nhất (",
                     format(start_date, "%d-%m-%Y"), " đến ", format(end_date, "%d-%m-%Y"), ")"),
       x = "Quốc gia", 
       y = "Số ca",
       fill = "Loại ca") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  scale_y_continuous(labels = scales::comma)


#7.biểu đồ hiển thị và so sánh tống số ca nhiễm và hồi phục của các châu lục (biểu đồ đường)
# Chuyển đổi tên quốc gia thành châu lục
covid_summary <- covid_summary %>%
  mutate(Continent = countrycode(Country, origin = "country.name", destination = "continent"))

# Tổng hợp số ca theo châu lục và ngày
continent_summary <- covid_summary %>%
  group_by(ObservationDate, Continent) %>%
  summarise(
    Total_Confirmed = sum(Total_Confirmed, na.rm = TRUE),
    Total_Recovered = sum(Total_Recovered, na.rm = TRUE)
  ) %>%
  drop_na()  # Loại bỏ giá trị NA

# Vẽ biểu đồ đường
ggplot(continent_summary, aes(x = ObservationDate)) +
  geom_line(aes(y = Total_Confirmed, color = Continent), size = 1) +
  geom_line(aes(y = Total_Recovered, color = Continent), size = 1, linetype = "dashed") +
  labs(title = "Tổng số ca nhiễm và hồi phục theo châu lục",
       x = "Ngày", y = "Số ca",
       color = "Châu lục") +
  scale_y_continuous(labels = scales::comma) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
#8. Biểu đồ hiển thị và so sánh tổng số ca nhiễm và hồi phục của các châu lục (biểu đồ nhiệt)

# Chuyển dữ liệu sang dạng dài với thời gian thực tế
continent_long <- continent_summary %>%
  pivot_longer(cols = c(Total_Confirmed, Total_Recovered), 
               names_to = "Type", 
               values_to = "Count") %>%
  mutate(ObservationDate = as.Date(ObservationDate))  # Đảm bảo định dạng ngày

# Kiểm tra dữ liệu
print(continent_long)

# Chuyển dữ liệu sang dạng bảng rộng để vẽ biểu đồ nhiệt
continent_wide <- continent_long %>%
  pivot_wider(names_from = Continent, values_from = Count, values_fill = list(Count = 0)) %>%
  arrange(ObservationDate)  # Sắp xếp theo ngày

# Kiểm tra dữ liệu đã chuyển đổi
print(continent_wide)

# Tạo ma trận từ dữ liệu
continent_matrix <- as.matrix(continent_wide[, -1])
continent_matrix <- apply(continent_matrix, 2, as.numeric)

# Định dạng nhãn thời gian
formatted_dates <- as.Date(continent_wide$ObservationDate)

# Kiểm tra ma trận có ít nhất 2 hàng và 2 cột không
if (nrow(continent_matrix) > 1 && ncol(continent_matrix) > 1) {
  # Tạo heatmap nếu có đủ dữ liệu
  heatmap(continent_matrix, 
          Rowv = NA, Colv = NA, 
          col = rev(heat.colors(256)), 
          scale = "none", 
          main = "Biểu đồ nhiệt số ca nhiễm và hồi phục theo châu lục", 
          xlab = "Châu lục", ylab = "Thời gian", 
          margins = c(8, 8), 
          cexRow = 1, cexCol = 0.7,
          labRow = format(formatted_dates, "%d-%m-%Y"))  # Hiển thị ngày tháng
} else {
  print("Dữ liệu không đủ để tạo biểu đồ nhiệt.")
}

# 9. Dự báo xu hướng lây nhiễm dựa trên các mô hình học máy

# Chọn dữ liệu để dự báo (sử dụng tổng số ca nhiễm theo ngày)
forecast_data <- covid_summary %>%
  group_by(ObservationDate) %>%
  summarise(Total_Confirmed = sum(Total_Confirmed, na.rm = TRUE)) %>%
  arrange(ObservationDate)

# Chuyển đổi dữ liệu thành chuỗi thời gian
ts_data <- ts(forecast_data$Total_Confirmed, start = c(year(min(forecast_data$ObservationDate)), month(min(forecast_data$ObservationDate))), frequency = 365)

# Chia dữ liệu thành tập huấn luyện và kiểm tra
train_data <- window(ts_data, end = c(year(max(forecast_data$ObservationDate)) - 1, 12))
test_data <- window(ts_data, start = c(year(max(forecast_data$ObservationDate)), 1))

# Áp dụng mô hình ARIMA để dự báo
fit_arima <- auto.arima(train_data)

# Dự báo cho tập kiểm tra
forecast_arima <- forecast(fit_arima, h = length(test_data))

# Vẽ biểu đồ dự báo
autoplot(forecast_arima) +
  autolayer(test_data, series = "Actual", PI = FALSE) +
  labs(title = "Dự báo xu hướng lây nhiễm COVID-19 bằng mô hình ARIMA",
       x = "Thời gian", y = "Tổng số ca nhiễm") +
  theme_minimal()

# Đánh giá mô hình
accuracy(forecast_arima, test_data)



# 10. Ngắt kết nối Spark
spark_disconnect(sc)
