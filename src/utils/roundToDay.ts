export function roundToDay(date: Date) {
    const d = new Date(date);
    d.setHours(0, 0, 0, 0);
    return d;
}
