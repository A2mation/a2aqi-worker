export function roundToHour(date: Date) {
    const d = new Date(date);
    d.setMinutes(0, 0, 0);
    return d;
}
