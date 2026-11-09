import {
  collection,
  doc,
  setDoc,
  getDocs,
  query,
  where,
  updateDoc,
  deleteDoc,
  serverTimestamp,
  onSnapshot,
  Timestamp,
} from 'firebase/firestore';
import { db } from './config';
import { Booking, UserProfile, Showtime } from '../types';

// ============================================================
// SEAT HOLD (Giữ ghế tạm thời 5 phút)
// ============================================================

const HOLD_DURATION_MS = 5 * 60 * 1000; // 5 phút = 300 000 ms

export interface SeatHold {
  holdId: string;         // ID định danh phiên giữ ghế (uuid ngẫu nhiên)
  showtimeId: string;
  seatLabels: string[];
  userId: string;
  expiresAt: number;      // Timestamp (ms) khi hết hạn
  createdAt: string;
}

/** Tạo holdId duy nhất */
const generateHoldId = () =>
  'hold_' + Math.random().toString(36).slice(2, 11) + '_' + Date.now();

/**
 * Giữ ghế tạm thời trên Firestore (collection: seat_holds).
 * Trả về holdId để sau này có thể giải phóng.
 */
export const holdSeatsInFirestore = async (
  showtime: Showtime,
  seatLabels: string[],
  userId: string
): Promise<string | null> => {
  if (!seatLabels.length) return null;

  // Dọn dẹp trước bất kỳ hold cũ nào của user này cho suất chiếu này
  await releaseHeldSeats(null, showtime.id, seatLabels, userId);

  const holdId = generateHoldId();
  const expiresAt = Date.now() + HOLD_DURATION_MS;

  const holdData: SeatHold = {
    holdId,
    showtimeId: showtime.id,
    seatLabels,
    userId,
    expiresAt,
    createdAt: new Date().toISOString(),
  };

  try {
    await setDoc(doc(db, 'seat_holds', holdId), holdData);
    // Lưu vào sessionStorage để chống mất holdId khi F5
    try {
      sessionStorage.setItem('cineviet_current_hold_id', holdId);
    } catch {}
    console.log('🔒 Đã giữ ghế tạm thời:', seatLabels, '| Hết hạn lúc:', new Date(expiresAt).toLocaleTimeString());
    return holdId;
  } catch (e) {
    console.warn('⚠️ Không thể tạo seat hold trên Firestore:', e);
    return null;
  }
};

/**
 * Giải phóng (xóa) giữ ghế trên Firestore.
 * Hỗ trợ linh hoạt: theo holdId, hoặc theo showtimeId + seatLabels + userId.
 */
export const releaseHeldSeats = async (
  holdIdOrParams?: string | null | {
    holdId?: string | null;
    showtimeId?: string;
    seatLabels?: string[];
    userId?: string;
  },
  showtimeIdArg?: string,
  seatLabelsArg?: string[],
  userIdArg?: string
): Promise<void> => {
  let holdId: string | null | undefined = null;
  let showtimeId: string | undefined = showtimeIdArg;
  let seatLabels: string[] | undefined = seatLabelsArg;
  let userId: string | undefined = userIdArg;

  if (typeof holdIdOrParams === 'object' && holdIdOrParams !== null) {
    holdId = holdIdOrParams.holdId;
    showtimeId = holdIdOrParams.showtimeId;
    seatLabels = holdIdOrParams.seatLabels;
    userId = holdIdOrParams.userId;
  } else {
    holdId = holdIdOrParams;
  }

  // Nếu không có holdId truyền vào, thử đọc từ sessionStorage
  if (!holdId) {
    try {
      holdId = sessionStorage.getItem('cineviet_current_hold_id');
    } catch {}
  }

  try {
    sessionStorage.removeItem('cineviet_current_hold_id');
  } catch {}

  const deletePromises: Promise<any>[] = [];
  const holdsRef = collection(db, 'seat_holds');

  // 1. Xóa trực tiếp theo holdId nếu có
  if (holdId) {
    deletePromises.push(deleteDoc(doc(db, 'seat_holds', holdId)));
  }

  // 2. Tìm và xóa triệt để theo showtimeId, seatLabels hoặc userId
  if (showtimeId) {
    try {
      const q = query(holdsRef, where('showtimeId', '==', showtimeId));
      const snap = await getDocs(q);
      const now = Date.now();

      snap.forEach((d) => {
        const data = d.data() as SeatHold;
        let shouldDelete = false;

        if (holdId && d.id === holdId) shouldDelete = true;
        if (userId && data.userId && data.userId === userId) shouldDelete = true;
        if (seatLabels && seatLabels.length > 0 && Array.isArray(data.seatLabels)) {
          if (data.seatLabels.some((lbl) => seatLabels!.includes(lbl))) {
            shouldDelete = true;
          }
        }
        // Xóa luôn các bản ghi đã quá hạn
        if (data.expiresAt && data.expiresAt < now) {
          shouldDelete = true;
        }

        if (shouldDelete) {
          deletePromises.push(deleteDoc(d.ref));
        }
      });
    } catch (err) {
      console.warn('⚠️ Lỗi truy vấn xóa seat_holds:', err);
    }
  }

  try {
    await Promise.all(deletePromises);
    if (deletePromises.length > 0) {
      console.log(`🔓 Đã giải phóng ${deletePromises.length} bản ghi seat_holds trên Firestore`);
    }
  } catch (e) {
    console.warn('⚠️ Lỗi xóa seat hold docs:', e);
  }
};

/**
 * Lắng nghe realtime các ghế đang được giữ (held) cho một suất chiếu.
 * Tự động loại bỏ hold của phiên hiện tại, tự động dọn dẹp theo thời gian thực (mỗi 2.5 giây).
 */
export const subscribeHeldSeatsForShowtime = (
  showtime: Showtime,
  currentHoldId: string | null,
  onUpdate: (heldLabels: string[]) => void,
  currentUserId?: string | null
): (() => void) => {
  try {
    const holdsRef = collection(db, 'seat_holds');
    const q = query(holdsRef, where('showtimeId', '==', showtime.id));

    let latestDocs: SeatHold[] = [];

    const computeAndEmit = () => {
      const now = Date.now();
      const heldSet = new Set<string>();

      latestDocs.forEach((hold) => {
        // Bỏ qua nếu đã hết hạn
        if (!hold.expiresAt || hold.expiresAt < now) return;
        // Bỏ qua nếu là hold của chính phiên này
        if (currentHoldId && hold.holdId === currentHoldId) return;
        // Bỏ qua nếu là hold của chính user này
        if (currentUserId && hold.userId && hold.userId === currentUserId) return;

        if (Array.isArray(hold.seatLabels)) {
          hold.seatLabels.forEach((lbl) => heldSet.add(lbl));
        }
      });

      onUpdate(Array.from(heldSet));
    };

    // Chu kỳ 2.5s tự động rà soát hết hạn để nhả ghế ngay trên UI của tất cả user
    const intervalTimer = setInterval(computeAndEmit, 2500);

    const unsubscribe = onSnapshot(
      q,
      (snapshot) => {
        const docsList: SeatHold[] = [];
        snapshot.forEach((d) => {
          docsList.push(d.data() as SeatHold);
        });
        latestDocs = docsList;
        computeAndEmit();
      },
      (err) => {
        console.warn('⚠️ Lỗi onSnapshot seat_holds:', err);
        onUpdate([]);
      }
    );

    return () => {
      clearInterval(intervalTimer);
      unsubscribe();
    };
  } catch (e) {
    return () => {};
  }
};

/**
 * Kiểm tra xem các ghế định đặt có đang bị người khác đặt hoặc giữ không
 * Trả về danh sách nhãn ghế bị xung đột (nếu có).
 */
export const checkSeatConflicts = async (
  showtime: Showtime,
  seatLabels: string[],
  currentUserId?: string | null,
  currentHoldId?: string | null,
  movieTitle?: string
): Promise<string[]> => {
  const conflicts = new Set<string>();

  // 1. Kiểm tra vé đã thanh toán
  try {
    const booked = await fetchBookedSeatsForShowtime(showtime, movieTitle);
    seatLabels.forEach((lbl) => {
      if (booked.includes(lbl)) conflicts.add(lbl);
    });
  } catch (err) {
    console.warn('⚠️ Lỗi kiểm tra vé đã thanh toán:', err);
  }

  // 2. Kiểm tra ghế đang bị người khác giữ
  try {
    const holdsRef = collection(db, 'seat_holds');
    const q = query(holdsRef, where('showtimeId', '==', showtime.id));
    const snap = await getDocs(q);
    const now = Date.now();

    snap.forEach((d) => {
      const hold = d.data() as SeatHold;
      // Bỏ qua nếu đã hết hạn
      if (!hold.expiresAt || hold.expiresAt < now) return;
      // Bỏ qua nếu là hold của chính phiên hoặc chính user này
      if (currentHoldId && hold.holdId === currentHoldId) return;
      if (currentUserId && hold.userId && hold.userId === currentUserId) return;

      if (Array.isArray(hold.seatLabels)) {
        hold.seatLabels.forEach((lbl) => {
          if (seatLabels.includes(lbl)) {
            conflicts.add(lbl);
          }
        });
      }
    });
  } catch (e) {
    console.warn('⚠️ Lỗi kiểm tra xung đột seat_holds:', e);
  }

  return Array.from(conflicts);
};

const LOCAL_STORAGE_KEY = 'cineviet_local_bookings';

/**
 * Lấy danh sách vé đã lưu trong LocalStorage (làm bộ đệm hoặc khi offline)
 */
export const getLocalBookings = (): Booking[] => {
  try {
    const data = localStorage.getItem(LOCAL_STORAGE_KEY);
    return data ? JSON.parse(data) : [];
  } catch {
    return [];
  }
};

/**
 * Lưu vé mới vào LocalStorage
 */
export const saveLocalBooking = (booking: Booking): void => {
  try {
    const current = getLocalBookings();
    const exists = current.some((b) => b.id === booking.id);
    if (!exists) {
      localStorage.setItem(LOCAL_STORAGE_KEY, JSON.stringify([booking, ...current]));
    }
  } catch (e) {
    console.warn('Không thể lưu vé vào LocalStorage:', e);
  }
};

/**
 * Cập nhật trạng thái vé trong LocalStorage
 */
export const updateLocalBookingStatus = (bookingId: string, status: Booking['status']): void => {
  try {
    const current = getLocalBookings();
    const updated = current.map((b) => (b.id === bookingId ? { ...b, status } : b));
    localStorage.setItem(LOCAL_STORAGE_KEY, JSON.stringify(updated));
  } catch (e) {
    console.warn('Không thể cập nhật vé trong LocalStorage:', e);
  }
};

/**
 * Lưu đơn đặt vé mới lên Cloud Firestore (kèm dự phòng LocalStorage)
 */
export const saveBookingToFirestore = async (
  booking: Booking,
  user?: UserProfile | null
): Promise<boolean> => {
  // Luôn lưu local trước để đảm bảo trải nghiệm người dùng
  saveLocalBooking(booking);

  try {
    const bookingDoc = {
      ...booking,
      userId: user?.uid || 'guest',
      userEmail: user?.email || '',
      userName: user?.displayName || 'Khách đặt vé',
      updatedAt: serverTimestamp(),
    };

    const docRef = doc(db, 'bookings', booking.id);
    await setDoc(docRef, bookingDoc);
    console.log('✅ Đã lưu vé thành công lên Firestore:', booking.id);
    return true;
  } catch (error) {
    console.warn('⚠️ Lỗi khi lưu vé lên Firestore (đã lưu dự phòng trong LocalStorage):', error);
    return false;
  }
};

/**
 * Lấy danh sách vé của người dùng từ Firestore (kết hợp với vé LocalStorage)
 */
export const fetchUserBookings = async (user?: UserProfile | null): Promise<Booking[]> => {
  const localList = getLocalBookings();

  if (!user?.email && !user?.uid) {
    return localList;
  }

  try {
    const bookingsRef = collection(db, 'bookings');
    let snapshot;

    // Tìm theo userId hoặc email của người dùng
    if (user.uid && user.uid !== '1') {
      const q = query(bookingsRef, where('userId', '==', user.uid));
      snapshot = await getDocs(q);
    }

    if ((!snapshot || snapshot.empty) && user.email) {
      const qEmail = query(bookingsRef, where('userEmail', '==', user.email));
      snapshot = await getDocs(qEmail);
    }

    const firestoreList: Booking[] = [];
    if (snapshot && !snapshot.empty) {
      snapshot.forEach((d) => {
        firestoreList.push(d.data() as Booking);
      });
    }

    // Hợp nhất dữ liệu giữa Firestore và LocalStorage (ưu tiên không trùng ID)
    const map = new Map<string, Booking>();
    firestoreList.forEach((b) => map.set(b.id, b));
    localList.forEach((b) => {
      if (!map.has(b.id)) {
        map.set(b.id, b);
      }
    });

    return Array.from(map.values());
  } catch (error) {
    console.warn('⚠️ Không thể tải vé từ Firestore, sử dụng danh sách Local:', error);
    return localList;
  }
};

/**
 * Hủy vé trên Firestore và LocalStorage
 */
export const cancelBookingInFirestore = async (bookingId: string): Promise<boolean> => {
  updateLocalBookingStatus(bookingId, 'cancelled');

  try {
    const bookingRef = doc(db, 'bookings', bookingId);
    await updateDoc(bookingRef, {
      status: 'cancelled',
      cancelledAt: serverTimestamp(),
    });
    console.log('✅ Đã cập nhật hủy vé trên Firestore:', bookingId);
    return true;
  } catch (error) {
    console.warn('⚠️ Không thể cập nhật trạng thái hủy lên Firestore:', error);
    return false;
  }
};

/**
 * Kiểm tra xem một vé đặt có thuộc về suất chiếu cụ thể hay không
 */
const isBookingMatchShowtime = (b: Booking, st: Showtime, movieTitle?: string): boolean => {
  if (b.status === 'cancelled') return false;
  // Ưu tiên so khớp ID suất chiếu trực tiếp
  if (b.showtimeId) {
    return b.showtimeId === st.id;
  }
  // So khớp dự phòng nếu vé cũ chưa có showtimeId
  const sameDate = b.date === st.date;
  const sameTime = b.startTime === st.startTime;
  const sameCinema = b.cinemaName ? b.cinemaName === st.cinemaName : false;
  const sameRoom = b.roomName ? b.roomName === st.roomName : false;
  const sameMovie = movieTitle && b.movieTitle ? b.movieTitle === movieTitle : true;
  return sameDate && sameTime && sameCinema && sameRoom && sameMovie;
};

/**
 * Lấy danh sách các ghế đã được đặt cho một suất chiếu cụ thể (kết hợp Firestore và LocalStorage)
 */
export const fetchBookedSeatsForShowtime = async (
  showtime: Showtime,
  movieTitle?: string
): Promise<string[]> => {
  const bookedSet = new Set<string>();

  // 1. Kiểm tra LocalStorage
  const localBookings = getLocalBookings();
  localBookings.forEach((b) => {
    if (isBookingMatchShowtime(b, showtime, movieTitle) && b.seatLabels) {
      b.seatLabels.forEach((lbl) => bookedSet.add(lbl));
    }
  });

  // 2. Kiểm tra Firestore
  try {
    const bookingsRef = collection(db, 'bookings');
    const snapshot = await getDocs(bookingsRef);
    snapshot.forEach((d) => {
      const b = d.data() as Booking;
      if (isBookingMatchShowtime(b, showtime, movieTitle) && b.seatLabels) {
        b.seatLabels.forEach((lbl) => bookedSet.add(lbl));
      }
    });
  } catch (error) {
    console.warn('⚠️ Không thể lấy danh sách ghế đặt từ Firestore:', error);
  }

  return Array.from(bookedSet);
};

/**
 * Lắng nghe cập nhật ghế đã đặt theo thời gian thực (Real-time listener) từ Firebase Firestore
 */
export const subscribeBookedSeatsForShowtime = (
  showtime: Showtime,
  onUpdate: (bookedSeatLabels: string[]) => void,
  movieTitle?: string
): (() => void) => {
  // Đồng bộ ban đầu từ LocalStorage
  const getFromLocal = () => {
    const localBookings = getLocalBookings();
    const set = new Set<string>();
    localBookings.forEach((b) => {
      if (isBookingMatchShowtime(b, showtime, movieTitle) && b.seatLabels) {
        b.seatLabels.forEach((lbl) => set.add(lbl));
      }
    });
    return set;
  };

  try {
    const bookingsRef = collection(db, 'bookings');
    return onSnapshot(
      bookingsRef,
      (snapshot) => {
        const bookedSet = getFromLocal();
        snapshot.forEach((d) => {
          const b = d.data() as Booking;
          if (isBookingMatchShowtime(b, showtime, movieTitle) && b.seatLabels) {
            b.seatLabels.forEach((lbl) => bookedSet.add(lbl));
          }
        });
        onUpdate(Array.from(bookedSet));
      },
      (err) => {
        console.warn('⚠️ Lỗi onSnapshot Firestore booked seats:', err);
        onUpdate(Array.from(getFromLocal()));
      }
    );
  } catch (e) {
    onUpdate(Array.from(getFromLocal()));
    return () => {};
  }
};
