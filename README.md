# CovidAnalysis-R-Spark
---

## 1. Giới thiệu  

### Môn học Big Data  
Big Data (Dữ liệu lớn) là một lĩnh vực nghiên cứu về các phương pháp lưu trữ, xử lý và phân tích dữ liệu có quy mô lớn, tốc độ cao và đa dạng. Môn học này tập trung vào các công nghệ như **Apache Spark, Hadoop**, cũng như các thuật toán học máy và thống kê để khai thác thông tin từ dữ liệu khổng lồ.  
Môn học được hướng dẫn bởi hai giảng viên **Trần Quý Nam** và **Lê Thị Thùy Trang**, thuộc khoa *Công Nghệ Thông Tin*, trường **Đại học Đại Nam**.  

### Dự án CovidAnalysis-R-Spark  
Dự án này nhằm **phân tích dữ liệu COVID-19** sử dụng **Sparklyr và ggplot2 trong R**. Chương trình kết nối với **Apache Spark** để xử lý dữ liệu quy mô lớn và trực quan hóa dữ liệu với nhiều loại biểu đồ.  

### Phân cụm K-means  
K-means là một thuật toán phân cụm không giám sát, được sử dụng để **nhóm các điểm dữ liệu vào các cụm dựa trên sự tương đồng**. Trong dự án này, K-means có thể được sử dụng để **phân nhóm các quốc gia hoặc khu vực** dựa trên các chỉ số COVID-19 như **số ca nhiễm, số ca tử vong và tỷ lệ hồi phục**.  

### Mô hình ARIMA  
ARIMA (*AutoRegressive Integrated Moving Average*) là một mô hình dự báo chuỗi thời gian mạnh mẽ. Trong phân tích dữ liệu COVID-19, ARIMA có thể được áp dụng để **dự đoán số ca nhiễm trong tương lai** dựa trên dữ liệu lịch sử, giúp các nhà nghiên cứu và nhà hoạch định chính sách đưa ra quyết định tốt hơn.  

### Thành viên nhóm  
Nhóm thực hiện dự án gồm 4 thành viên:  

| Họ và Tên | Mã Sinh Viên |  
|-----------|-------------|  
| Nguyễn Hữu Huy | 1671020139 |  
| Nguyễn Thanh Bình | 1671020041 |  
| Nguyễn Xuân Thuận | 1671010307 |  
| Đào Thị Phương Long | 1671020182 |  

---

## 2. Dữ liệu  

Dữ liệu được thu thập từ **Kaggle**, chứa thông tin về tình hình dịch COVID-19 trên toàn cầu  trong khoảng thời gian 22-01-2020 đến 29-05-2021. Tập dữ liệu có **306,429 dòng và 8 cột**, bao gồm các thông tin như **ngày quan sát, quốc gia/khu vực, số ca nhiễm, tử vong và hồi phục**.  

Một số điểm cần lưu ý khi xử lý dữ liệu:  

- **Cột "Province/State"** có nhiều giá trị bị thiếu, cần xử lý trước khi phân tích.  
- **Dữ liệu thời gian** đang ở dạng chuỗi (string), cần được chuyển đổi sang định dạng thời gian phù hợp nếu thực hiện phân tích chuỗi thời gian với ARIMA.  
- **Các cột "Confirmed", "Deaths" và "Recovered"** là kiểu số, có thể sử dụng cho phân tích thống kê, trực quan hóa và phân cụm với K-means.  

---

## 3. Cài đặt

### 3.1. Cài đặt thư viện

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

## 4. Kết nối với Spark

Chương trình kết nối với Apache Spark chạy ở chế độ local:

```r
sc <- spark_connect(master = "local")
```

## 5. Nạp và Tiền xử lý dữ liệu

- Dữ liệu COVID-19 được tải từ file CSV và Spark tự động nhận diện schema.
- Chuyển đổi kiểu dữ liệu của các cột.
- Loại bỏ các cột không cần thiết.

```r
covid_cleaned <- covid_data %>%
  mutate(
    ObservationDate = to_timestamp(ObservationDate, "MM/dd/yyyy"),
    Country = trimws(CountryRegion),
    Province = ifelse(is.na(ProvinceState) | ProvinceState == "", "Unknown", ProvinceState)
  ) %>%
  select(-Last_Update)
```

## 6. Phân tích Dữ liệu

### 6.1. Phân tích thống kê

Tính tỷ lệ tử vong và hồi phục:

```r
covid_summary <- covid_summary %>%
  mutate(
    Fatality_Rate = ifelse(Total_Confirmed > 0, Total_Deaths / Total_Confirmed, 0),
    Recovery_Rate = ifelse(Total_Confirmed > 0, Total_Recovered / Total_Confirmed, 0)
  )
```

### 6.2. Phân cụm K-Means

### 6.2.1. Xác định số cụm tối ưu bằng phương pháp Elbow

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
```
![image](https://github.com/user-attachments/assets/775fd475-1418-41b4-9e67-df84711b9a0c)
- Dữ liệu được chuẩn hóa trước khi phân cụm.
- Sử dụng phương pháp K-Means với 3 cụm được tối ưu bằng Elbow.

```r
set.seed(123)
kmeans_result <- kmeans(covid_scaled, centers = 3, nstart = 10)
```
**Biểu đồ:** Trực quan hóa kết quả phân cụm:

```r
fviz_cluster(kmeans_result, data = covid_scaled, geom = "point", ellipse.type = "convex") +
  scale_color_manual(values = cluster_colors, aesthetics = "colour") +  # Gán màu đúng
  scale_fill_manual(values = cluster_colors, aesthetics = "fill") +  # Đảm bảo màu cũng áp dụng cho fill
  labs(title = "Kết quả phân cụm các quốc gia theo mức độ ảnh hưởng dịch COVID-19") +
  theme_minimal() +
  theme(legend.position = "top")
```

![image](https://github.com/user-attachments/assets/6e133fc1-6c28-4c82-a253-bcb1c83f8010)

**Biểu đồ:** Thể hiện phần trăm ca nhiễm của từng cụm:

```r
ggplot(cluster_cases, aes(x = Cluster, y = Percentage, fill = Cluster)) +
  geom_bar(stat = "identity", color = "black", width = 0.6) +  # Thu nhỏ chiều rộng cột
  scale_fill_manual(values = cluster_colors) +  # Áp dụng màu sắc theo cụm
  geom_text(aes(label = paste0(round(Percentage, 1), "%\n(", format(Total_Cases, big.mark = "."), " ca)")),
            vjust = -0.3, size = 4) +  # Giảm size chữ để vừa vặn
  ylim(0, max(cluster_cases$Percentage) + 5) +  # Điều chỉnh trục y để nhãn không bị cắt
  labs(title = "Tỷ lệ phần trăm ca nhiễm của các cụm",
       x = "Cụm", y = "Phần trăm ca nhiễm (%)") +
  theme_minimal()
```

![image](https://github.com/user-attachments/assets/74e2e142-ca36-4a26-8735-99600a929b16)

### 6.3. Biểu đồ số ca nhiễm theo thời gian

```r
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
```

![image](https://github.com/user-attachments/assets/c2625af9-3275-4d90-83e5-e86f306e7e14)

## 7. Trực quan hóa Dữ liệu

### 7.1. Biểu đồ phân tán số ca nhiễm và tử vong

```r
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
```

![image](https://github.com/user-attachments/assets/e23783cc-98c7-463a-987f-fdc0987e5988)

### 7.2. Heatmap mức độ tập trung dịch bệnh

**Biểu đồ:** Heatmap cho 10 quốc gia có số ca nhiễm cao nhất:

```r
plot_heatmap(c(top_10_countries, "Total_Confirmed"), "Heatmap - 10 quốc gia nhiễm cao nhất", "Total_Confirmed", "Total_Confirmed")
```

![image](https://github.com/user-attachments/assets/6183003f-9902-4cca-9840-9ee27bd73c62)

**Biểu đồ:** Heatmap cho 10 quốc gia có số tử vong cao nhất:

```r
plot_heatmap(c(top_10_death_countries, "Total_Deaths"), "Heatmap - 10 quốc gia có số ca tử vong cao nhất", "Total_Deaths", "Total_Deaths")
```

![image](https://github.com/user-attachments/assets/e97a7a13-9d9d-48e3-b58e-5c1afe645355)

### 7.3. Biểu đồ hiển thị tổng số ca mắc, hồi phục và tử vong của 10 quốc gia có số ca mắc cao nhất

**Biểu đồ:** hiển thị tổng số ca mắc, hồi phục và tử vong của 10 quốc gia có số ca mắc cao nhất:

```r
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
```

![image](https://github.com/user-attachments/assets/88137dd2-edca-43d4-94c1-c4e0358f9e40)

### 7.4. Biểu đồ hiển thị tổng số ca mắc, hồi phục và tử vong của 10 quốc gia có số ca mắc thấp nhất

**Biểu đồ:** hiển thị tổng số ca mắc, hồi phục và tử vong của 10 quốc gia có số ca mắc thấp nhất:

```r
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
```

![image](https://github.com/user-attachments/assets/b6251021-0ba7-4200-8bb8-fbbb8ae4fd16)

### 7.5. Biểu đồ hiển thị và so sánh tổng số ca nhiễm và hồi phục của các châu lục

**Biểu đồ:** hiển thị và so sánh tổng số ca nhiễm và hồi phục của các châu lục:

```r
ggplot(continent_summary, aes(x = ObservationDate)) +
  geom_line(aes(y = Total_Confirmed, color = Continent), size = 1) +
  geom_line(aes(y = Total_Recovered, color = Continent), size = 1, linetype = "dashed") +
  labs(title = "Tổng số ca nhiễm và hồi phục theo châu lục",
       x = "Ngày", y = "Số ca",
       color = "Châu lục") +
  scale_y_continuous(labels = scales::comma) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
```

![image](https://github.com/user-attachments/assets/23f7c1f2-98ab-40af-8487-5324a675ad44)

### 7.6. Biểu đồ hiển thị và so sánh tổng số ca nhiễm và hồi phục của các châu lục (biểu đồ nhiệt)

**Biểu đồ:** hiển thị và so sánh tổng số ca nhiễm và hồi phục của các châu lục (biểu đồ nhiệt):

```r
heatmap(continent_matrix,
        Rowv = NA, Colv = NA,
        col = rev(heat.colors(256)),
        scale = "none",
        main = "Biểu đồ nhiệt số ca nhiễm và hồi phục theo châu lục",
        xlab = "Châu lục", ylab = "Thời gian",
        margins = c(8, 8),
        cexRow = 1, cexCol = 0.7,
        labRow = format(formatted_dates, "%d-%m-%Y"))
```

![image](https://github.com/user-attachments/assets/1dc7c003-340d-413d-b551-da0b03fd9964)

## 8. Dự báo xu hướng lây nhiễm COVID-19 bằng mô hình ARIMA:

```r
# Vẽ biểu đồ dự báo
forecast::autoplot(forecast_result) +
  labs(
    title = "Dự báo xu hướng lây nhiễm COVID-19 (30 ngày tiếp theo)",
    x = "Ngày",
    y = "Số ca nhiễm",
    subtitle = "Dự báo dựa trên mô hình ARIMA"
  ) +
  theme_minimal()
```
![image](https://github.com/user-attachments/assets/a82bd825-42b6-4fb3-a584-422e5e99e936)

## 9. Kết luận

Dự án giúp trực quan hóa và phân tích dữ liệu COVID-19 với Sparklyr và ggplot2. Phân cụm K-Means giúp phân loại quốc gia theo mức độ ảnh hưởng của đại dịch, dự đoán các ca nhiễm trong tương lai bằng mô hình ARIMA. Các biểu đồ hỗ trợ đánh giá xu hướng dịch bệnh và sự ảnh hưởng trên toàn cầu.
