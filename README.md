
Dưới đây là phiên bản README hoàn chỉnh, đã được cập nhật theo mã trên và giữ lại các hình ảnh (được biểu diễn dưới dạng placeholder Markdown) từ phiên bản cũ:

---

# CovidAnalysis-R-Spark

=======
# CovidAnalysis-R-Spark - DNU

---

## 1. Giới thiệu

### Môn học Big Data

Big Data (Dữ liệu lớn) là lĩnh vực nghiên cứu các phương pháp lưu trữ, xử lý và phân tích dữ liệu có quy mô lớn, tốc độ cao và đa dạng. Môn học tập trung vào các công nghệ như **Apache Spark** và **Hadoop**, cùng với các thuật toán học máy và thống kê để khai thác thông tin từ dữ liệu khổng lồ.  
Môn học được hướng dẫn bởi giảng viên **Trần Quý Nam** và **Lê Thị Thùy Trang** thuộc khoa _Công Nghệ Thông Tin_, trường **Đại học Đại Nam**.

### Dự án CovidAnalysis-R-Spark

Dự án này nhằm **phân tích dữ liệu COVID-19** sử dụng **sparklyr** để kết nối với Apache Spark và **ggplot2** để trực quan hóa dữ liệu trong R. Các bước chính của dự án bao gồm:

- Đọc dữ liệu từ file CSV và đẩy lên Spark
- Tiền xử lý dữ liệu trên Spark
- Tổng hợp số liệu theo quốc gia và ngày
- Phân cụm các quốc gia với thuật toán K-Means
- Trực quan hóa kết quả phân cụm và các biểu đồ khác
- Dự báo số ca nhiễm trong tương lai bằng mô hình ARIMA

### Phân cụm K-Means

K-Means là thuật toán phân cụm không giám sát, giúp nhóm các điểm dữ liệu (ví dụ: các quốc gia) dựa trên sự tương đồng về các chỉ số như số ca nhiễm, tử vong và hồi phục.

### Mô hình ARIMA

ARIMA (_AutoRegressive Integrated Moving Average_) là mô hình dự báo chuỗi thời gian mạnh mẽ. Trong dự án, ARIMA được áp dụng để dự báo số ca nhiễm COVID-19 trong tương lai dựa trên dữ liệu lịch sử.

### Thành viên nhóm

| Họ và Tên           | Mã Sinh Viên |
| ------------------- | ------------ |
| Nguyễn Hữu Huy      | 1671020139   |
| Nguyễn Thanh Bình   | 1671020041   |
| Nguyễn Xuân Thuận   | 1671010307   |
| Đào Thị Phương Long | 1671020182   |

---

## 2. Dữ liệu

Dữ liệu được thu thập từ **Kaggle** chứa thông tin về tình hình dịch COVID-19 toàn cầu từ ngày **22-01-2020** đến **29-05-2021**.  
Tập dữ liệu có **306,429 dòng và 8 cột**, bao gồm các thông tin như:

- Ngày quan sát
- Quốc gia/Khu vực
- Số ca nhiễm
- Số ca tử vong
- Số ca hồi phục

**Lưu ý khi xử lý dữ liệu:**

- Cột "Province/State" có nhiều giá trị thiếu, cần xử lý.
- Dữ liệu thời gian đang ở dạng chuỗi (string) và cần chuyển sang kiểu timestamp để thực hiện phân tích chuỗi thời gian.
- Các cột "Confirmed", "Deaths" và "Recovered" là số và được sử dụng cho phân tích thống kê, trực quan hóa và phân cụm.

---

## 3. Cài đặt

### 3.1. Cài đặt các thư viện

Chạy lệnh sau để cài đặt các thư viện cần thiết:

```r
install.packages(c("sparklyr", "dplyr", "ggplot2", "cluster", "factoextra",
                   "forecast", "tidyr", "scales", "lubridate", "pheatmap", "countrycode"))
```

### 3.2. Cài đặt Apache Spark (nếu chưa có)

```r
if (!"3.4.1" %in% spark_available_versions()) {
  spark_install(version = "3.4.1")
}
```

---

## 4. Kết nối với Spark

Chương trình kết nối với Apache Spark chạy ở chế độ local:

```r
spark_disconnect_all()
sc <- spark_connect(master = "local")
```

---

## 5. Nạp và Tiền xử lý dữ liệu

- Dữ liệu COVID-19 được tải từ file CSV, Spark tự động nhận diện schema.
- Chuyển đổi cột ngày sang kiểu timestamp, làm sạch tên quốc gia và xử lý giá trị thiếu của cột Province/State.

```r
covid_data <- spark_read_csv(sc, name = "covid", path = "C:/Users/Admin/Downloads/covid_19_data.csv",
                             infer_schema = TRUE, header = TRUE)
glimpse(covid_data)

covid_cleaned <- covid_data %>%
  mutate(
    ObservationDate = to_timestamp(ObservationDate, "MM/dd/yyyy"),
    Country = trimws(CountryRegion),
    Province = ifelse(is.na(ProvinceState) | ProvinceState == "", "Unknown", ProvinceState)
  ) %>%
  select(-Last_Update)
```

Tổng hợp số liệu theo quốc gia và ngày:

```r
covid_summary_spark <- covid_cleaned %>%
  group_by(ObservationDate, Country) %>%
  summarise(
    Total_Confirmed = sum(Confirmed, na.rm = TRUE),
    Total_Deaths = sum(Deaths, na.rm = TRUE),
    Total_Recovered = sum(Recovered, na.rm = TRUE)
  )
covid_summary <- covid_summary_spark %>% collect()
```

Tính toán các chỉ số thống kê:

```r
covid_summary <- covid_summary %>%
  mutate(
    Fatality_Rate = ifelse(Total_Confirmed > 0, Total_Deaths / Total_Confirmed, 0),
    Recovery_Rate = ifelse(Total_Confirmed > 0, Total_Recovered / Total_Confirmed, 0)
  )
```

---

## 6. Phân tích Dữ liệu

### 6.1. Phân cụm K-Means trên Spark

- Xác định ngày quan sát mới nhất:

```r
max_date <- covid_cleaned %>%
  summarise(max_date = max(ObservationDate)) %>%
  collect() %>%
  .$max_date
```

- Lấy dữ liệu mới nhất theo từng quốc gia:

```r
covid_latest_spark <- covid_cleaned %>%
  filter(ObservationDate == max_date) %>%
  group_by(Country) %>%
  summarise(
    Total_Confirmed = sum(Confirmed, na.rm = TRUE),
    Total_Deaths = sum(Deaths, na.rm = TRUE),
    Total_Recovered = sum(Recovered, na.rm = TRUE)
  )
```

- Tạo vector đặc trưng với tên "input_features" để tránh xung đột:

```r
covid_features <- covid_latest_spark %>%
  ft_vector_assembler(
    input_cols = c("Total_Confirmed", "Total_Deaths", "Total_Recovered"),
    output_col = "input_features"
  )
```

- Áp dụng mô hình K-Means với 3 cụm:

```r
set.seed(123)
k <- 3
kmeans_model_spark <- covid_features %>%
  ml_kmeans(~ input_features, k = k, seed = 123)
covid_clustered_spark <- ml_predict(kmeans_model_spark, covid_features)
covid_clustered_local <- covid_clustered_spark %>%
  select(Country, Total_Confirmed, Total_Deaths, Total_Recovered, prediction) %>%
  collect()
print(covid_clustered_local)
```

### 6.2. Phân tích và Trực quan hóa trên R

#### 6.2.1. Phân tích cụm với phương pháp Elbow và trực quan hóa phân cụm

- Lấy dữ liệu mới nhất và chuẩn hóa:

```r
covid_latest <- covid_summary %>%
  filter(ObservationDate == max(ObservationDate)) %>%
  select(Country, Total_Confirmed, Total_Deaths, Total_Recovered) %>%
  drop_na()

covid_scaled <- covid_latest %>%
  select(-Country) %>%
  scale()
```

- Tính WCSS và vẽ đồ thị Elbow:

```r
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
```

![image](https://github.com/user-attachments/assets/775fd475-1418-41b4-9e67-df84711b9a0c)

- Phân cụm với k = 3 và trực quan hóa:

```r
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
```

![image](https://github.com/user-attachments/assets/6e133fc1-6c28-4c82-a253-bcb1c83f8010)

- Vẽ biểu đồ cột cho tỷ lệ ca nhiễm theo cụm:

```r
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
```

![image](https://github.com/user-attachments/assets/74e2e142-ca36-4a26-8735-99600a929b16)

#### 6.2.2. Biểu đồ số ca nhiễm theo thời gian

```r
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
```

![image](https://github.com/user-attachments/assets/c2625af9-3275-4d90-83e5-e86f306e7e14)

#### 6.2.3. Biểu đồ phân tán số ca nhiễm và tử vong

```r
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
```

![image](https://github.com/user-attachments/assets/e23783cc-98c7-463a-987f-fdc0987e5988)

#### 6.2.4. Heatmap mức độ tập trung dịch bệnh

- Lấy danh sách quốc gia và tổng hợp số liệu:

```r
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
```

- Hàm vẽ heatmap:

```r
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
```

- Vẽ heatmap:

```r
# Heatmap cho 10 quốc gia có số ca mắc cao nhất
plot_heatmap(c(top_10_countries, "Total_Confirmed"), "Heatmap - 10 quốc gia nhiễm cao nhất", "Total_Confirmed", "Total_Confirmed")
```

![image](https://github.com/user-attachments/assets/6183003f-9902-4cca-9840-9ee27bd73c62)

```r
# Heatmap cho 10 quốc gia có số ca tử vong cao nhất
plot_heatmap(c(top_10_death_countries, "Total_Deaths"), "Heatmap - 10 quốc gia có số ca tử vong cao nhất", "Total_Deaths", "Total_Deaths")
```

![image](https://github.com/user-attachments/assets/e97a7a13-9d9d-48e3-b58e-5c1afe645355)

#### 6.2.5. Biểu đồ hiển thị tổng số ca mắc, hồi phục và tử vong của 10 quốc gia có số ca mắc cao nhất

```r
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
```

![image](https://github.com/user-attachments/assets/88137dd2-edca-43d4-94c1-c4e0358f9e40)

#### 6.2.6. Biểu đồ hiển thị tổng số ca mắc, hồi phục và tử vong của 10 quốc gia có số ca mắc thấp nhất

```r
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
```

![image](https://github.com/user-attachments/assets/b6251021-0ba7-4200-8bb8-fbbb8ae4fd16)

#### 6.2.7. Biểu đồ đường so sánh số ca nhiễm và hồi phục theo châu lục

- Xử lý tên quốc gia không khớp:

```r
covid_summary <- covid_summary %>%
  mutate(Country = case_when(
    Country %in% c("Channel Islands", "Diamond Princess", "Kosovo",
                   "Micronesia", "MS Zaandam", "North Ireland", "Others", "St. Martin") ~ NA_character_,
    TRUE ~ Country
  )) %>%
  mutate(Continent = countrycode(Country, origin = "country.name", destination = "continent"))
```

- Tổng hợp và vẽ biểu đồ đường:

```r
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
  scale_y_continuous(labels = scales::label_comma()) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
```

![image](https://github.com/user-attachments/assets/23f7c1f2-98ab-40af-8487-5324a675ad44)

#### 6.2.8. Biểu đồ nhiệt so sánh số ca nhiễm và hồi phục theo châu lục

```r
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
```

![image](https://github.com/user-attachments/assets/1dc7c003-340d-413d-b551-da0b03fd9964)

#### 6.2.9. Dự báo xu hướng lây nhiễm COVID-19 bằng mô hình ARIMA

```r
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
```

![image](https://github.com/user-attachments/assets/a82bd825-42b6-4fb3-a584-422e5e99e936)

---

## 7. Kết nối và Kết thúc

Sau khi hoàn thành phân tích và trực quan hóa, ngắt kết nối với Spark:

```r
spark_disconnect(sc)
```

---

## 8. Kết luận

Dự án CovidAnalysis-R-Spark cho phép trực quan hóa và phân tích dữ liệu COVID-19 bằng Sparklyr và ggplot2 trong R.

- **Phân cụm K-Means:** Giúp phân nhóm các quốc gia theo mức độ ảnh hưởng của dịch bệnh dựa trên số ca nhiễm, tử vong và hồi phục.
- **Mô hình ARIMA:** Dự báo xu hướng ca nhiễm trong tương lai, hỗ trợ các quyết định của các nhà hoạch định chính sách.
- **Trực quan hóa:** Các biểu đồ phân cụm, heatmap, biểu đồ đường và biểu đồ cột cung cấp cái nhìn tổng quan về dữ liệu COVID-19 trên toàn cầu.

_Lưu ý:_ Các biểu đồ do ggplot2 tạo sẽ hiển thị trong cửa sổ “Plots” của RStudio, còn Spark UI (http://localhost:4040) cung cấp thông tin về tiến trình và hiệu năng của Spark.
