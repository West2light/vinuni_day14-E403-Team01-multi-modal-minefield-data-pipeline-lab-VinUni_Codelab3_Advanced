# Hướng dẫn Sinh viên: The Multi-Modal Minefield (Data Pipeline Lab)

## 🎯 Giới thiệu
Chào mừng bạn đến với Lab Xây dựng Data Pipeline thực tế. Trong bài học này, bạn sẽ xây dựng một pipeline thu nạp dữ liệu mạnh mẽ cho một Cơ sở tri thức (Knowledge Base - KB). Bạn sẽ phải xử lý các vấn đề thực tế: định dạng không nhất quán, dữ liệu "độc hại" (toxic data) và các quy tắc nghiệp vụ ẩn.

---

## 🏗 Cấu trúc Dự án
- `raw_data/`: Chứa các file nguồn (PDF, CSV, HTML, TXT, PY). **Không di chuyển các file này.**
- `starter_code/`: Điểm bắt đầu của bạn. Thực hiện logic của bạn tại đây.
- `solution_code/`: (Tham khảo sau buổi học) Một bản thực hiện hoàn chỉnh.
- `forensic_agent/`: Chứa `agent_forensic.py` để kiểm tra kết quả cuối cùng của bạn.

---

## 👥 Nhóm & Vai trò

### 1. Lead Data Architect (Role 1)
- **File chính**: `schema.py`
- **Mục tiêu**: Định nghĩa `UnifiedDocument` bằng Pydantic.
- **Thử thách**: Dự đoán sự thay đổi. Vào phút thứ 60, "khách hàng" sẽ yêu cầu thay đổi schema (v2). Hãy sẵn sàng để di chuyển dữ liệu.

### 2. ETL/ELT Builder (Role 2)
- **File chính**: `process_pdf.py`, `process_csv.py`, `process_html.py`, `process_transcript.py`, `process_legacy_code.py`
- **Mục tiêu**: Trích xuất dữ liệu sạch, có cấu trúc từ các nguồn hỗn loạn.
- **Nhiệm vụ chính**: Sử dụng **Gemini API** để xử lý file `lecture_notes.pdf`.

### 3. Observability & QA Engineer (Role 3)
- **File chính**: `quality_check.py`
- **Mục tiêu**: Viết các "Cổng kiểm soát ngữ nghĩa" (Semantic Gates).
- **Nhiệm vụ**:
    - Loại bỏ các chuỗi "độc hại" (ví dụ: thông báo lỗi).
    - Phát hiện sự sai lệch (ví dụ: logic trong bình luận khác với giá trị thực tế).

### 4. DevOps & Integration Specialist (Role 4)
- **File chính**: `orchestrator.py`
- **Mục tiêu**: Kết nối tất cả các phần thành một DAG (Directed Acyclic Graph) duy nhất.
- **Nhiệm vụ**:
    - Import và gọi các hàm xử lý.
    - Lưu kết quả cuối cùng thành `processed_knowledge_base.json` trong thư mục gốc.
    - Đo thời gian xử lý (theo dõi SLA).

---

## 🛠 Gợi ý Kỹ thuật

### 📍 Đường dẫn file (Quan trọng!)
Chúng tôi đã xử lý sẵn các đường dẫn file cho bạn trong `starter_code`.
- Trong `orchestrator.py`, hãy sử dụng hằng số `RAW_DATA_DIR`.
- Trong mỗi file `process_*.py`, hãy sử dụng đối số `file_path` được cung cấp cho hàm.
- **Không viết cứng đường dẫn tuyệt đối** (ví dụ: `C:\Users\...`), vì chúng sẽ bị lỗi trên máy tính khác.

### 🤖 Gemini API (Trích xuất PDF)
Đối với file PDF, bạn không thể sử dụng trích xuất văn bản đơn giản vì nó chứa các bảng và bố cục đặc thù.
```python
# Gợi ý cho Role 2:
pdf_file = genai.upload_file(path=file_path)
response = model.generate_content([pdf_file, "Extract Title, Author, and a 3-sentence summary."])
```

### 🔑 Hướng dẫn lấy Gemini API Key và thiết lập `.env`
Để sử dụng Gemini API trong phần trích xuất PDF, bạn cần có một API Key:
1. Truy cập [Google AI Studio](https://aistudio.google.com/app/apikey).
2. Đăng nhập bằng tài khoản Google của bạn.
3. Nhấn **"Create API key"** để tạo và sao chép mã khóa.
4. Tạo một file mới có tên là `.env` ở thư mục gốc của dự án.
5. Thêm dòng sau vào file `.env` (thay thế bằng key thực tế của bạn):
   ```env
   GEMINI_API_KEY=your_api_key_here
   ```
   *(Lưu ý: Đảm bảo code của bạn có sử dụng `python-dotenv` để load biến môi trường này hoặc bạn có thể set trực tiếp trên terminal)*

### 🧹 Dọn dẹp dữ liệu hỗn loạn
- **CSV**: Chú ý các ID trùng lặp và định dạng giá như "$1200" so với "500000".
- **Transcript**: Loại bỏ dấu thời gian như `[00:05:12]` và tiếng ồn như `[Music]`.
- **Legacy Code**: Sử dụng module `ast` để đọc docstring mà không cần chạy code.

---

## ✅ Cách kiểm tra bài làm
1. Chạy pipeline của bạn:
   ```bash
   python starter_code/orchestrator.py
   ```
2. Chạy Forensic Agent để chấm điểm đầu ra:
   ```bash
   python forensic_agent/agent_forensic.py
   ```

---

## ⚠️ Sự cố giữa buổi Lab
Vào lúc **11:00 AM**, một bản tin khẩn cấp sẽ được phát ra. Schema đang thay đổi. Bạn phải cập nhật toàn bộ code để hỗ trợ các tên trường mới mà không làm mất dữ liệu. **Sự phối hợp giữa Role 1 và Role 2 là cực kỳ quan trọng!**
