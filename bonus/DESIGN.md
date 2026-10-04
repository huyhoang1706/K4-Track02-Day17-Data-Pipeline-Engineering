# B2 — Pipeline flywheel cho chatbot chăm sóc khách hàng tiếng Việt

## Bài toán và phạm vi bằng chứng

Thiết kế này đề xuất cách mở rộng chatbot hỗ trợ khách hàng trong lab: biến trace, phản hồi và kết quả xử lý ticket thành bộ đánh giá và dữ liệu huấn luyện có thể kiểm tra nguồn gốc. Người dùng đầu ra là người phụ trách chất lượng chatbot và kỹ sư mô hình; khách hàng cuối cần câu trả lời đúng chính sách, không phải chỉ một câu trả lời trôi chảy. Đây là kịch bản thiết kế, chưa phải yêu cầu đã xác nhận qua phỏng vấn hay hệ thống production đã triển khai.

**Quan sát từ repo:** trace là cây span; bộ seed có 8 trace, 21 span, 2 mẫu eval và 3 cặp ưu tiên thô. `make flywheel` loại 2 cặp trùng eval, còn 1 cặp. Ví dụ ASOF cho thấy join lấy giá trị mới nhất làm rò rỉ tương lai ở 2 hàng. `make kg` cho thấy câu hỏi widget giao từ đâu cần nối hai quan hệ qua accessory. Những kết quả này chứng minh cơ chế trên seed, không chứng minh độ chính xác trên tiếng Việt thực tế.

**Giả định để ra quyết định:** giai đoạn đầu có một nhóm nhỏ vận hành, dữ liệu nằm trong một miền quyền truy cập, khoảng 10.000 lượt hội thoại mỗi ngày và có thể chấp nhận dữ liệu đánh giá mới vào sáng hôm sau. Các con số là mục tiêu thiết kế chưa đo. Nếu cần cập nhật mô hình trong vài giây hoặc có nhiều bên cùng ghi, phải xem lại kiến trúc.

## 1. Nguồn dữ liệu nào đủ tin cậy để tạo nhãn?

Nguồn gồm trace JSON của agent, CDC ticket, phản hồi của khách và phiên bản chính sách. Mỗi bản ghi cần `trace_id`, `span_id`, thời điểm sự kiện, thời điểm nhận, phiên bản agent và nguồn. Bronze lưu payload với quyền đọc hạn chế; Silver kiểm schema, loại bản giao lại theo khoá và đưa dòng thiếu định danh vào quarantine. Span con được làm phẳng đệ quy, nhưng phải giữ quan hệ cha để phân biệt lỗi gọi công cụ với câu trả lời cuối.

Quyết định: không dùng trạng thái kỹ thuật `ok` làm nhãn chất lượng duy nhất. Một API chạy thành công vẫn có thể trả lời sai. Mẫu được dùng làm reference phải có người kiểm tra nội dung và phiên bản chính sách; cặp DPO phải cùng câu hỏi và có bằng chứng câu nào tốt hơn. Đổi lại, duyệt thủ công chậm và tốn công hơn nhãn tự động. Tôi chọn chất lượng nhãn trước số lượng vì dữ liệu sai có thể khiến lần huấn luyện tiếp theo củng cố lỗi của chatbot.

## 2. Batch hay streaming và dữ liệu đến muộn xử lý thế nào?

Quyết định ban đầu là batch hằng đêm, chạy tuần tự trên DuckDB, xuất tập dữ liệu đã duyệt theo version. So với streaming, batch giảm số dịch vụ và dễ tái lập khi điều tra một nhãn sai. Đổi lại, phản hồi hôm nay chưa xuất hiện trong bộ dữ liệu của buổi sáng hôm nay; đây là đánh đổi phù hợp với giả định độ tươi một ngày.

Ticket có thể được đóng sau hội thoại nhiều ngày; điện thoại mất mạng có thể gửi phản hồi muộn. Đo phân phối lateness riêng cho từng nguồn thay vì áp dụng mặc định ba ngày của seed cho production. Chọn lookback theo P99 đo được và theo dõi số bản ghi nằm ngoài cửa sổ. Dữ liệu quá muộn đi vào danh sách partition cần backfill, không bị bỏ âm thầm. Khi làm tập train, phân biệt thời điểm sự kiện với thời điểm thông tin thực sự có sẵn để tránh dùng kết quả xử lý tương lai cho một quyết định trong quá khứ.

## 3. Làm sao ngăn eval rò rỉ vào train?

Quyết định: chia holdout theo nhóm hội thoại trước khi tạo cặp ưu tiên, sau đó kiểm trùng prompt và trùng nội dung với eval. Chuẩn hoá Unicode NFC, chữ hoa/thường và khoảng trắng để so sánh; bản gốc đã xử lý PII vẫn được giữ để đánh giá cách viết có dấu. Exact match rẻ, dễ giải thích nhưng không bắt câu hỏi viết lại. Vì vậy, bước tiếp theo là kiểm n-gram hoặc tương đồng với ngưỡng hiệu chỉnh trên mẫu do người duyệt; không giả định một ngưỡng dùng tốt cho mọi câu tiếng Việt.

So với chia ngẫu nhiên từng lượt, chia theo hội thoại giảm nguy cơ một câu hỏi và câu hỏi nối tiếp xuất hiện ở hai tập. Đổi lại, tập nhỏ có thể lệch chủ đề. Báo cáo độ phủ theo nhóm lỗi, chủ đề và phiên bản chính sách để thấy khoảng trống. Feature lịch sử dùng ASOF với thời điểm khả dụng, không join giá trị mới nhất. Bản manifest lưu phiên bản dữ liệu, quy tắc chia và checksum để một metric có thể truy lại đúng tập đã chấm.

## 4. PII và yêu cầu xoá lan đến đâu?

Quyết định: đặt cổng PII ở Bronze → Silver trước khi nội dung được đưa vào eval, train, cache hoặc dịch vụ gán nhãn. Regex email/điện thoại được kết hợp với nhận diện tên, địa chỉ và định danh theo ngữ cảnh tiếng Việt. Mẫu nghi ngờ được quarantine để người có quyền duyệt. Đo precision/recall theo từng loại PII trên tập gán nhãn, vì chỉ đếm số chuỗi bị che không cho biết số trường hợp bị bỏ sót.

Giữ dữ liệu gốc giúp debug nhưng tăng phạm vi cần bảo vệ. Tôi chọn lưu Bronze hạn chế quyền, có thời hạn giữ và liên kết tới định danh phục vụ xoá. Yêu cầu xoá tạo tombstone có thứ tự thay đổi, rồi lập danh sách mọi bản sao liên quan: transcript, snapshot, chunks, cache, bản xuất và bản sao lưu. Snapshot đã thu hồi không tiếp tục được cấp cho huấn luyện; phát hành version sạch mới và ghi metadata audit thay vì giữ nguyên văn bản cá nhân trong log. Đây là thiết kế vận hành cần được chủ dữ liệu xác nhận, chưa phải cơ chế xoá đã triển khai trong lab.

## 5. Chạy lại và chi phí tăng theo quy mô được kiểm soát thế nào?

Quyết định: mỗi batch có định danh cố định; Silver merge theo khoá với guard theo thứ tự thay đổi; Gold ghi lại partition hoặc phát hành snapshot mới. Bước LLM dùng cache theo hash đầu vào, model và prompt version, validate JSON trước khi ghi Gold; phản hồi sai schema được lưu quarantine và cache kết quả thất bại để replay không gọi lại vô hạn. Chỉ thử lại lỗi schema khi có thay đổi phiên bản hoặc thao tác rõ ràng. Dự toán token cho phần cache miss trước khi chạy để người vận hành quyết định ngân sách.

Ở quy mô 10 lần, đo thời gian đọc, số file nhỏ, số cache miss và độ dài hàng đợi duyệt trước khi đổi công nghệ. Gom file theo partition khi cần, giới hạn số mẫu chờ duyệt bằng cách ưu tiên nhóm lỗi và thêm lấy mẫu ngẫu nhiên để tránh thiên lệch. Ở quy mô 100 lần, kiểm tra giới hạn RAM, thời gian backfill và yêu cầu ghi đồng thời; chỉ khi đo được thiếu năng lực mới chuyển compute hay storage. Không đưa tiền dự toán của FakeLLM trong lab thành chi phí nhà cung cấp thực.

**Phương án bị loại:** cập nhật mô hình trực tiếp sau mỗi phản hồi và dùng mọi lượt `ok` làm dữ liệu đúng. Phương án này nhanh nhưng trộn phản hồi chưa kiểm chứng, có nguy cơ tự củng cố lỗi và khó tái lập. Kafka + Spark cũng chưa được chọn cho giai đoạn đầu vì độ tươi giả định không cần streaming; đổi lại, thiết kế batch chưa đáp ứng SLA vài giây.

## Kiến trúc đề xuất

```text
Trace JSON + ticket CDC + feedback + policy versions
                         |
                 Bronze hạn chế quyền
                         |
        Silver: schema / dedup / PII / tombstone
                         |
         point-in-time join + nhóm hội thoại
                         |
             nhãn có người kiểm tra
                         |
            decontamination + manifest
                 /                 \
        eval holdout          train / DPO version
                 \                 /
           kiểm tra chất lượng trước phát hành
                         |
         người phụ trách quyết định dùng mô hình
```

Prototype tham chiếu là `extensions/flywheel.py` có sẵn: chạy được bằng `make flywheel` và minh hoạ quyết định decontamination cùng ASOF join. Tôi không nhận đây là prototype mới do mình viết, và không xem exact-match trong prototype là đủ cho production. Output thực tế nằm ở `submission/evidence/flywheel.txt`; thiết kế fuzzy matching, PII theo ngữ cảnh và quy trình duyệt ở trên chưa được triển khai.
